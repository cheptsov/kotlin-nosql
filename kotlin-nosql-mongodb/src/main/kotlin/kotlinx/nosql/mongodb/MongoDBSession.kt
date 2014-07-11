package kotlinx.nosql.mongodb

import com.mongodb.DB
import com.mongodb.BasicDBObject
import java.lang.reflect.Field
import java.util.ArrayList
import java.util.HashMap
import com.mongodb.DBObject
import java.util.Arrays
import org.bson.types.ObjectId
import com.mongodb.BasicDBList
import kotlinx.nosql.*
import kotlinx.nosql.util.*
import java.util.regex.Pattern
import java.util.Date
import java.util.concurrent.atomic.AtomicReference
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import kotlinx.nosql.Session.DocumentSchemaQueryWrapper
import kotlinx.nosql.Session.DocumentSchemaQueryParams
import kotlinx.nosql.Session.TableSchemaProjectionQueryParams
import com.mongodb.DBCollection
import com.mongodb.DBCursor

class MongoDBSession(val db: DB) : Session() {
    val dbVersion : String
    val searchOperatorSupported: Boolean

    {
        val results = db.command("buildInfo")
        dbVersion = results!!.get("version")!!.toString()
        val versions = dbVersion.split('.')
        searchOperatorSupported = versions[0].toInt() >= 2 && versions[1].toInt() >= 6
    }

    fun ensureIndex(schema: Schema<*>, index: Index) {
        val collection = db.getCollection(schema.schemaName)!!
        val dbObject = BasicDBObject()
        for (column in index.ascending) {
            dbObject.append(column.name, 1)
        }
        for (column in index.descending) {
            dbObject.append(column.name, -1)
        }
        for (column in index.text) {
            dbObject.append(column.name, "text")
        }
        if (index.name.isNotEmpty())
            collection.ensureIndex(dbObject, index.name)
        else
            collection.ensureIndex(dbObject)
    }

    override fun <T : AbstractTableSchema> T.create() {
        db.createCollection(this.schemaName, null)
    }

    override fun <T : AbstractTableSchema> T.drop() {
        val collection = db.getCollection(this.schemaName)!!
        collection.remove(BasicDBObject())
    }

    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T> {
        val collection = db.getCollection(this.schemaName)!!
        val doc = getDBObject(v, this)
        if (discriminator != null) {
            var dominatorValue: Any? = null
            for (entry in DocumentSchema.discriminatorClasses.entrySet()) {
                if (entry.value.equals(v.javaClass)) {
                    dominatorValue = entry.key.value
                }
            }
            doc.set(this.discriminator.column.name, dominatorValue!!)
        }
        collection.insert(doc)
        return Id<P, T>(doc.get("_id").toString() as P)
    }

    private fun getDBObject(o: Any, schema: Any): BasicDBObject {
        val doc = BasicDBObject()
        val javaClass = o.javaClass
        val fields = getAllFields(javaClass)
        var sc: Class<out Any?>? = null
        var s: AbstractSchema? = null
        if (schema is DocumentSchema<*, *> && schema.discriminator != null) {
            for (entry in DocumentSchema.discriminatorClasses.entrySet()) {
                if (entry.value.equals(o.javaClass)) {
                    sc = DocumentSchema.discriminatorSchemaClasses.get(entry.key)!!
                    s = DocumentSchema.discriminatorSchemas.get(entry.key)!!
                }
            }
        }
        val schemaClass: Class<out Any?> = if (schema is DocumentSchema<*, *> && schema.discriminator != null) sc!! else schema.javaClass
        val objectSchema: Any = if (schema is DocumentSchema<*, *> && schema.discriminator != null) s!! else schema
        val schemaFields = getAllFieldsMap(schemaClass as Class<in Any>, { f -> f.isColumn })
        for (field in fields) {
            val schemaField = schemaFields.get(field.getName()!!.toLowerCase())
            if (schemaField != null && schemaField.isColumn) {
                field.setAccessible(true)
                schemaField.setAccessible(true)
                val column = schemaField.asColumn(objectSchema)
                val value = field.get(o)
                if (value != null) {
                    if (column.columnType.primitive) {
                        doc.append(column.name, when (value) {
                            is DateTime, is LocalDate, is LocalTime -> value.toString()
                            is Id<*, *> -> ObjectId(value.value.toString())
                            else -> value
                        })
                    } else if (column.columnType.iterable) {
                        val list = BasicDBList()
                        for (v in (value as Iterable<Any>)) {
                            list.add(if (column.columnType.custom) getDBObject(v, column) else
                                (if (v is Id<*, *>) ObjectId(v.toString()) else v))
                        }
                        doc.append(column.name, list)
                    } else doc.append(column.name, getDBObject(value, column))
                }
            }
        }
        return doc
    }

    override fun <T : DocumentSchema<P, C>, P, C> find(params: DocumentSchemaQueryParams<T, P, C>): Iterator<C> {
        return object:Iterator<C> {
            var cursor: DBCursor? = null
            var pos = 0
            override fun next(): C {
                if (cursor == null) {
                    val collection = db.getCollection(params.schema.schemaName)
                    val query = if (params.query != null) getQuery(params.query) else BasicDBObject()
                    cursor = collection!!.find(query)!!
                    if (params.skip != null) {
                        cursor!!.skip(params.skip!!)
                    }
                }
                val value = getObject(cursor!!.next(), params.schema) as C
                pos++
                if (!cursor!!.hasNext() || (params.take != null && pos == params.take!!)) {
                    cursor!!.close()
                    pos = -1
                }
                return value
            }
            override fun hasNext(): Boolean {
                if (cursor == null) {
                    val collection = db.getCollection(params.schema.schemaName)
                    val query = if (params.query != null) getQuery(params.query) else BasicDBObject()
                    cursor = collection!!.find(query)!!
                    if (params.skip != null) {
                        cursor!!.skip(params.skip!!)
                    }
                }
                return pos != -1 && cursor!!.hasNext() && (params.take == null || pos < params.take!!)
            }
        }
    }

    override fun <T : TableSchema<P>, P, V> find(params: TableSchemaProjectionQueryParams<T, P, V>): Iterator<V> {
        // TODO TODO TODO
        /*val collection = db.getCollection(params.table.schemaName)!!

        val cursor = collection.find(if (params.query != null) getQuery(params.query) else BasicDBObject(), fields)!!
        if (params.skip != null) {
            cursor.skip(params.skip!!)
        }
        try {
            var size = 0
            while (cursor.hasNext()) {
                val doc = cursor.next()
                val values = ArrayList<Any?>()
                params.projection.forEach {
                    values.add(getColumnObject(doc, it))
                }
                when (values.size) {
                    1 -> observer!!.onNext(values[0] as V)
                    2 -> observer!!.onNext(Pair(values[0], values[1]) as V)
                    3 -> observer!!.onNext(Triple(values[0], values[1], values[2]) as V)
                    4 -> observer!!.onNext(Quadruple(values[0], values[1], values[2], values[3]) as V)
                    5 -> observer!!.onNext(Quintuple(values[0], values[1], values[2], values[3], values[4]) as V)
                    6 -> observer!!.onNext(Sextuple(values[0], values[1], values[2], values[3], values[4], values[5]) as V)
                    7 -> observer!!.onNext(Septuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6]) as V)
                    8 -> observer!!.onNext(Octuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]) as V)
                    9 -> observer!!.onNext(Nonuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8]) as V)
                    10 -> observer!!.onNext(Decuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9]) as V)
                }
                if (params.take != null && ++size == params.take!!) {
                    break;
                }
            }
            observer!!.onCompleted()
        } catch (e: Throwable) {
            observer!!.onError(e)
        } finally {
            cursor.close();
        }
        return Subscriptions.empty()!!*/
        return object:Iterator<V> {
            var cursor: DBCursor? = null
            var pos = 0
            override fun next(): V {
                if (cursor == null) {
                    val collection = db.getCollection(params.table.schemaName)
                    val fields = BasicDBObject()
                    params.projection.forEach {
                        fields.append(it.fullName, "1")
                    }
                    val query = if (params.query != null) getQuery(params.query) else BasicDBObject()
                    cursor = collection!!.find(query, fields)!!
                    if (params.skip != null) {
                        cursor!!.skip(params.skip!!)
                    }
                }
                val doc = cursor!!.next()
                val values = ArrayList<Any?>()
                params.projection.forEach {
                    values.add(getColumnObject(doc, it))
                }
                val value = when (values.size) {
                    1 -> values[0] as V
                    2 -> Pair(values[0], values[1]) as V
                    3 -> Triple(values[0], values[1], values[2]) as V
                    4 -> Quadruple(values[0], values[1], values[2], values[3]) as V
                    5 -> Quintuple(values[0], values[1], values[2], values[3], values[4]) as V
                    6 -> Sextuple(values[0], values[1], values[2], values[3], values[4], values[5]) as V
                    7 -> Septuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6]) as V
                    8 -> Octuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]) as V
                    9 -> Nonuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8]) as V
                    10 -> Decuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9]) as V
                    else -> throw UnsupportedOperationException()
                }
                pos++
                if (!cursor!!.hasNext() || (params.take != null && pos == params.take!!)) {
                    cursor!!.close()
                    pos = -1
                }
                return value
            }
            override fun hasNext(): Boolean {
                if (cursor == null) {
                    val collection = db.getCollection(params.table.schemaName)
                    val fields = BasicDBObject()
                    params.projection.forEach {
                        fields.append(it.fullName, "1")
                    }
                    val query = if (params.query != null) getQuery(params.query) else BasicDBObject()
                    cursor = collection!!.find(query, fields)!!
                    if (params.skip != null) {
                        cursor!!.skip(params.skip!!)
                    }
                }
                return pos != -1 && cursor!!.hasNext() && (params.take == null || pos < params.take!!)
            }
        }
    }

    private fun Op.usesSearch(): Boolean {
        return when (this) {
            is TextOp -> true
            is OrOp -> this.expr1.usesSearch() || this.expr2.usesSearch()
            is AndOp -> this.expr1.usesSearch() || this.expr2.usesSearch()
            else -> false
        }
    }

    protected fun getQuery(op: Op, removePrefix: String = ""): BasicDBObject {
        val query = BasicDBObject()
        when (op) {
            is EqualsOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr1.columnType.primitive) {
                            if (op.expr1.columnType.id) {
                                query.append(op.expr1.fullName, ObjectId(op.expr2.value.toString()))
                            } else {
                                var columnName = op.expr1.fullName
                                if (removePrefix.isNotEmpty() && columnName.startsWith(removePrefix)) {
                                    columnName = columnName.substring(removePrefix.length + 1)
                                }
                                query.append( columnName, op.expr2.value)
                            }
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${op.expr1.fullName} == this.${op.expr2.fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is MatchesOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is Pattern) {
                            query.append(op.expr1.fullName, BasicDBObject().append("\$regex", op.expr2.value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is NotEqualsOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is String || op.expr2.value is Int) {
                            if (op.expr1.columnType.id) {
                                query.append(op.expr1.fullName, BasicDBObject().append("\$ne", ObjectId(op.expr2.value.toString())))
                            } else {
                                query.append(op.expr1.fullName, BasicDBObject().append("\$ne", op.expr2.value))
                            }
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${op.expr1.fullName} != this.${op.expr2.fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is GreaterOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is String || op.expr2.value is Int) {
                            query.append(op.expr1.fullName, BasicDBObject().append("\$gt", op.expr2.value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${op.expr1.fullName} > this.${op.expr2.fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is LessOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is String || op.expr2.value is Int) {
                            query.append(op.expr1.fullName, BasicDBObject().append("\$lt", op.expr2.value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${op.expr1.fullName} < this.${op.expr2.fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is GreaterEqualsOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is String || op.expr2.value is Int) {
                            query.append(op.expr1.fullName, BasicDBObject().append("\$gte", op.expr2.value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${op.expr1.fullName} >= this.${op.expr2.fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is LessEqualsOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is String || op.expr2.value is Int) {
                            query.append(op.expr1.fullName, BasicDBObject().append("\$lte", op.expr2.value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${op.expr1.fullName} <= this.${op.expr2.fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is InOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is List<*> || op.expr2.value is Array<*>) {
                            query.append(op.expr1.fullName, BasicDBObject().append("\$in", op.expr2.value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is NotInOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is List<*> || op.expr2.value is Array<*>) {
                            query.append(op.expr1.fullName, BasicDBObject().append("\$nin", op.expr2.value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            // TODO TODO TODO eq expression and eq expression
            is AndOp -> {
                val query1 = getQuery(op.expr1)
                val query2 = getQuery(op.expr2)
                for (entry in query1.entrySet()) {
                    query.append(entry.key, entry.value)
                }
                for (entry in query2.entrySet()) {
                    query.append(entry.key, entry.value)
                }
                return query
            }
            is OrOp -> {
                query.append("\$or", Arrays.asList(getQuery(op.expr1), getQuery(op.expr2)))
            }
            is TextOp -> {
                query.append("\$text", BasicDBObject().append("\$search", op.search))
            }
            is NoOp -> {
                // Do nothing
            }
            else -> {
                throw UnsupportedOperationException()
            }
        }
        return query
    }

    private fun <T: DocumentSchema<P, V>, P, V> getObject(doc: DBObject, schema: T): V {
        var s: AbstractSchema? = null
        val valueInstance: Any = if (schema is DocumentSchema<*, *> && schema.discriminator != null) {
            var instance: Any? = null
            val discriminatorValue = doc.get(schema.discriminator.column.name)
            for (discriminator in DocumentSchema.tableDiscriminators.get(schema.schemaName)!!) {
                if (discriminator.value.equals(discriminatorValue)) {
                    instance = newInstance(DocumentSchema.discriminatorClasses.get(discriminator)!!)
                    s = DocumentSchema.discriminatorSchemas.get(discriminator)!!
                    break
                }
            }
            instance!!
        } else {
            s = schema
            newInstance(schema.valueClass)
        }
        val schemaClass = s.javaClass
        val schemaFields = getAllFields(schemaClass as Class<in Any?>)
        val valueFields = getAllFieldsMap(valueInstance.javaClass as Class<in Any?>)
        for (schemaField in schemaFields) {
            if (javaClass<AbstractColumn<Any?, T, Any?>>().isAssignableFrom(schemaField.getType()!!)) {
                val valueField = valueFields.get(if (schemaField.getName()!!.equals("pk")) "id" else schemaField.getName()!!.toLowerCase())
                if (valueField != null) {
                    schemaField.setAccessible(true)
                    valueField.setAccessible(true)
                    val column = schemaField.asColumn(s!!)
                    val value = doc.get(column.name)
                    val columnValue: Any? = if (value == null) {
                        null
                    } else if (column.columnType.id && !column.columnType.iterable)
                        Id<P, T>(value.toString() as P)
                    else if (column.columnType.primitive) {
                        when (column.columnType) {
                            ColumnType.DATE -> LocalDate(value.toString())
                            ColumnType.TIME -> LocalTime(value.toString())
                            ColumnType.DATE_TIME -> DateTime(value.toString())
                            else -> doc.get(column.name)
                        }
                    } else if (column.columnType.list && !column.columnType.custom) {
                        (doc.get(column.name) as BasicDBList).toList()
                    } else if (column.columnType.set && !column.columnType.custom && !column.columnType.id) {
                        (doc.get(column.name) as BasicDBList).toSet()
                    } else if (column.columnType.id && column.columnType.set) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { Id<String, TableSchema<String>>(it.toString()) }.toSet()
                    } else if (column.columnType.custom && column.columnType.set) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as DBObject, column as ListColumn<*, out AbstractSchema>) }.toSet()
                    } else if (column.columnType.custom && column.columnType.list) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as DBObject, column as ListColumn<*, out AbstractSchema>) }.toList()
                    } else {
                        getObject(doc.get(column.name) as DBObject, column as Column<Any?, T>)
                    }
                    if (columnValue != null || column is AbstractNullableColumn) {
                        valueField.set(valueInstance, columnValue)
                    } else {
                        throw NullPointerException()
                    }
                }
            }
        }
        return valueInstance as V
    }

    private fun newInstance(clazz: Class<out Any?>): Any {
        val constructor = clazz.getConstructors()[0]
        val constructorParamTypes = constructor.getParameterTypes()!!
        val constructorParamValues = Array<Any?>(constructor.getParameterTypes()!!.size, { index ->
            when (constructorParamTypes[index].getName()) {
                "int" -> 0
                "java.lang.String" -> ""
                "org.joda.time.LocalDate" -> LocalDate()
                "org.joda.time.LocalTime" -> LocalTime()
                "org.joda.time.DateTime" -> DateTime()
                "double" -> 0.toDouble()
                "float" -> 0.toFloat()
                "long" -> 0.toLong()
                "short" -> 0.toShort()
                "byte" -> 0.toByte()
                "boolean" -> false
                "java.util.List" -> listOf<Any>()
                "java.util.Set" -> setOf<Any>()
                else -> newInstance(constructorParamTypes[index])
            }
        })
        return constructor.newInstance(*constructorParamValues)!!
    }

    private fun getObject(doc: DBObject, column: AbstractColumn<*, *, *>): Any? {
        val valueInstance = newInstance(column.valueClass)
        val schemaClass = column.javaClass
        val columnFields = schemaClass.getDeclaredFields()
        val valueFields = getAllFieldsMap(valueInstance.javaClass as Class<in Any?>)
        for (columnField in columnFields) {
            if (columnField.isColumn) {
                val valueField = valueFields.get(columnField.getName()!!.toLowerCase())
                if (valueField != null) {
                    columnField.setAccessible(true)
                    valueField.setAccessible(true)
                    val column = columnField.asColumn(column)
                    val columnValue: Any? = if (column.columnType.id && !column.columnType.iterable) Id<String, TableSchema<String>>(doc.get(column.name).toString())
                    else if (column.columnType.primitive) doc.get(column.name)
                    else if (column.columnType.list && !column.columnType.custom) (doc.get(column.name) as BasicDBList).toList()
                    else if (column.columnType.set && !column.columnType.custom && !column.columnType.id) (doc.get(column.name) as BasicDBList).toSet()
                    else if (column.columnType.custom && column.columnType.list) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as DBObject, column as ListColumn<*, out AbstractSchema>) }.toList()
                    } else if (column.columnType.id && column.columnType.set) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { Id<String, TableSchema<String>>(it.toString()) }.toSet()
                    } else if (column.columnType.custom && column.columnType.set) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as DBObject, column as ListColumn<*, out AbstractSchema>) }.toSet()
                    } else {
                        getObject(doc.get(column.name) as DBObject, column as Column<*, out AbstractSchema>)
                    }
                    if (columnValue != null || column is AbstractNullableColumn) {
                        valueField.set(valueInstance, columnValue)
                    } else {
                        throw NullPointerException()
                    }
                }
            }
        }
        return valueInstance
    }

    override fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<out Any?, T, out Any?>, Any?>>) {
        throw UnsupportedOperationException()
    }

    override fun <T : AbstractSchema> delete(table: T, op: Op): Int {
        val collection = db.getCollection(table.schemaName)!!
        val query = getQuery(op)
        return collection.remove(query)!!.getN()
    }

    override fun update(schema: AbstractSchema, columnValues: Array<Pair<AbstractColumn<*, *, *>, *>>, op: Op): Int {
        val collection = db.getCollection(schema.schemaName)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append("\$set", statement)
        for ((column, value) in columnValues) {
            statement.append(column.fullName, getDBValue(value, column))
        }
        return collection.update(getQuery(op), doc)!!.getN()
    }

    override fun <T> addAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Op): Int {
        val collection = db.getCollection(schema.schemaName)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append("\$pushAll", statement)
        statement.append(column.fullName, getDBValue(values, column))
        return collection.update(getQuery(op), doc)!!.getN()
    }

    override fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Op): Int {
        val collection = db.getCollection(schema.schemaName)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append("\$pullAll", statement)
        statement.append(column.fullName, getDBValue(values, column))
        return collection.update(getQuery(op), doc)!!.getN()
    }

    override fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, removeOp: Op, op: Op): Int {
        val collection = db.getCollection(schema.schemaName)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append("\$pull", statement)
        statement.append(column.fullName, getQuery(removeOp, column.fullName))
        return collection.update(getQuery(op), doc)!!.getN()
    }

    private fun getDBValue(value: Any?, column: AbstractColumn<*, *, *>): Any? {
        return if (!column.columnType.custom)
            when (value) {
                is DateTime, is LocalDate, is LocalTime -> value.toString()
                is Id<*, *> -> ObjectId(value.value.toString())
                else -> value
            }
        else if (column.columnType.custom && !column.columnType.iterable)
            if (value != null) getDBObject(value, column) else null
        else
            (value as List<*>).map { getDBObject(it!!, column) }
    }

    private fun getColumnObject(doc: DBObject, column: AbstractColumn<*, *, *>): Any? {
        val columnObject = parse(doc, column.fullName.split("\\."))
        return if (column.columnType.id) {
            Id<String, TableSchema<String>>(columnObject.toString())
        } else if (column.columnType.primitive) when (column.columnType) {
            ColumnType.DATE -> LocalDate.parse(columnObject.toString())
            ColumnType.TIME -> LocalTime.parse(columnObject.toString())
            ColumnType.DATE_TIME -> DateTime.parse(columnObject.toString())
            else -> columnObject
        } else if (!column.columnType.custom && column.columnType.set) {
            (columnObject as BasicDBList).toSet()
        } else if (!column.columnType.custom && column.columnType.list) {
            (columnObject as BasicDBList).toList()
        } else if (column.columnType.custom && column.columnType.list) {
            (columnObject as BasicDBList).map { getObject(it as DBObject, column as ListColumn<Any?, out AbstractSchema>) }
        } else if (column.columnType.custom && column.columnType.set) {
            (columnObject as BasicDBList).map { getObject(it as DBObject, column as ListColumn<Any?, out AbstractSchema>) }.toSet()
        } else if (column.columnType.custom) {
            getObject(columnObject as DBObject, column)
        } else {
            UnsupportedOperationException()
        }
    }

    private fun parse(doc: DBObject, path: Array<String>, position: Int = 0): Any? {
        val value = doc.get(path[position])
        if (position < path.size - 1) {
            return parse(value as DBObject, path, position + 1)
        } else {
            return value
        }
    }

    override fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, out Any?>): C {
        throw UnsupportedOperationException()
    }
    override fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, out Any?>): Int {
        throw UnsupportedOperationException()
    }
    override fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, out Any?>, v: C) {
        throw UnsupportedOperationException()
    }
}