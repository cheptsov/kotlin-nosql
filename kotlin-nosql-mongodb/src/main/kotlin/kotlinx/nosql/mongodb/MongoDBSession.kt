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

class PaginatedIterator<X>(val callback: (drop: Int?, take: Int?) -> Iterator<X>) : Iterator<X> {
    var drop: Int? = null
    var take: Int? = null

    var iterator:Iterator<X>? = null

    override fun next(): X {
        if (iterator == null) {
            iterator = callback(drop, take)
        }
        return iterator!!.next()
    }
    override fun hasNext(): Boolean {
        if (iterator == null) {
            iterator = callback(drop, take)
        }
        return iterator!!.hasNext()
    }
    fun drop(n: Int) : PaginatedIterator<X> {
        drop = n
        return this
    }
    fun take(n: Int) : PaginatedIterator<X> {
        take = n
        return this
    }
}

class MongoDBSession(val db: DB) : Session() {
    override fun <T : AbstractTableSchema> T.create() {
        throw UnsupportedOperationException()
    }

    override fun <T : AbstractTableSchema> T.drop() {
        val collection = db.getCollection(this.name)!!
        collection.remove(BasicDBObject())
    }

    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T> {
        val collection = db.getCollection(this.name)!!
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
        var s: Schema? = null
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
                            list.add(if (column.columnType.custom) getDBObject(v, column) else v)
                        }
                        doc.append(column.name, list)
                    } else doc.append(column.name, getDBObject(value, column))
                }
            }
        }
        return doc
    }

    // TODO TODO TODO Real iterator instead of list
    override fun <T : DocumentSchema<P, C>, P, C> T.filter(op: T.() -> Op): PaginatedIterator<C> {
        val collection = db.getCollection(this.name)!!
        val query = getQuery(op())
        return PaginatedIterator({ drop, take ->
            val cursor = collection.find(query)!!
            if (drop != null) {
                cursor.skip(drop)
            }
            val docs = ArrayList<C>()
            try {
                while (cursor.hasNext()) {
                    val doc = cursor.next()
                    val obj = getObject(doc, this)
                    docs.add(obj)
                    if (take != null && docs.size == take) break
                }
                docs.iterator()
            } finally {
                cursor.close();
            }
        })
    }

    /*override fun <T: AbstractTableSchema, A: AbstractColumn<List<C>, T, C>, C>iterator(rangeQuery: RangeQuery<T, A, C>): Iterator<C> {
       throw new UnsupportedOperationException()
    }*/

    private fun getQuery(op: Op, removePrefix: String = ""): BasicDBObject {
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
            else -> {
                throw UnsupportedOperationException()
            }
        }
        return query
    }

    private fun <T: DocumentSchema<P, V>, P, V> getObject(doc: DBObject, schema: T): V {
        var s: Schema? = null
        val valueInstance: Any = if (schema is DocumentSchema<*, *> && schema.discriminator != null) {
            var instance: Any? = null
            val discriminatorValue = doc.get(schema.discriminator.column.name)
            for (discriminator in DocumentSchema.tableDiscriminators.get(schema.name)!!) {
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
                    } else if (column.columnType.id)
                        Id<P, T>(value.toString() as P)
                    else if (column.columnType.primitive) {
                        when (column.columnType) {
                            ColumnType.DATE -> LocalDate(value.toString())
                            ColumnType.TIME -> LocalTime(value.toString())
                            ColumnType.DATE_TIME -> DateTime(value.toString())
                            else -> doc.get(column.name)
                        }
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
                    val columnValue: Any? = if (column.columnType.id) Id<String, TableSchema<String>>(doc.get(column.name).toString())
                    else if (column.columnType.primitive) doc.get(column.name)
                    else if (column.columnType.list && !column.columnType.custom) (doc.get(column.name) as BasicDBList).toList()
                    else if (column.columnType.set && !column.columnType.custom) (doc.get(column.name) as BasicDBList).toSet()
                    else if (column.columnType.custom && column.columnType.list) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as DBObject, column as ListColumn<*, out Schema>) }
                    } else if (column.columnType.custom && column.columnType.list) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as DBObject, column as ListColumn<*, out Schema>) }.toSet()
                    } else {
                        getObject(doc.get(column.name) as DBObject, column as Column<*, out Schema>)
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

    override fun <T : Schema> insert(columns: Array<Pair<AbstractColumn<out Any?, T, out Any?>, Any?>>) {
        throw UnsupportedOperationException()
    }
    override fun <T : Schema> delete(table: T, op: Op) {
        val collection = db.getCollection(table.name)!!
        val query = getQuery(op)
        collection.remove(query)
    }
    override fun <T : AbstractTableSchema, A: AbstractColumn<C, T, out Any?>, C> Query1<T, A, C>.set(c: C) {
        update(array(Pair(a, c)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.set(av: A, bv: B) {
        update(array(Pair(a, av), Pair(b, bv)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B, C> Query3<T, A, B, C>.set(av: A, bv: B, cv: C) {
        update(array(Pair(a, av), Pair(b, bv), Pair(c, cv)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B, C, D> Query4<T, A, B, C, D>.set(av: A, bv: B, cv: C, dv: D) {
        update(array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B, C, D, E> Query5<T, A, B, C, D, E>.set(av: A, bv: B, cv: C, dv: D, ev: E) {
        update(array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv), Pair(e, ev)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B, C, D, E, F> Query6<T, A, B, C, D, E, F>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F) {
        update(array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv), Pair(e, ev), Pair(f, fv)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B, C, D, E, F, G> Query7<T, A, B, C, D, E, F, G>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G) {
        update(array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv), Pair(e, ev), Pair(f, fv), Pair(g, gv)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H> Query8<T, A, B, C, D, E, F, G, H>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H) {
        update(array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv), Pair(e, ev), Pair(f, fv), Pair(g, gv), Pair(h, hv)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, I> Query9<T, A, B, C, D, E, F, G, H, I>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H, iv: I) {
        update(array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv), Pair(e, ev), Pair(f, fv), Pair(g, gv), Pair(h, hv), Pair(i, iv)), op, "\$set")
    }
    override fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, I, J> Query10<T, A, B, C, D, E, F, G, H, I, J>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H, iv: I, jv: J) {
        update(array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv), Pair(e, ev), Pair(f, fv), Pair(g, gv), Pair(h, hv), Pair(i, iv), Pair(j, jv)), op, "\$set")
    }

    private fun update(columnValues: Array<Pair<AbstractColumn<*, *, *>, *>>, op: Op, operator: String) {
        val collection = db.getCollection(Schema.current<Schema>().name)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append(operator, statement)
        for ((column, value) in columnValues) {
            statement.append(column.fullName, getDBValue(value, column))
        }
        collection.update(getQuery(op), doc)
    }

    private fun getDBValue(value: Any?, column: AbstractColumn<*, *, *>): Any? {
        return if (!column.columnType.custom)
            value
        else if (column.columnType.custom && !column.columnType.iterable)
            if (value != null) getDBObject(value, column) else null
        else
            (value as List<*>).map { getDBObject(it!!, column) }
    }

/*
    override fun <T : AbstractTableSchema, C> AbstractColumn<C, T, out Any?>.forEach(statement: (C) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, C> AbstractColumn<C, T, out Any?>.iterator(): Iterator<C> {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, C, M> AbstractColumn<C, T, out Any?>.map(statement: (C) -> M): List<M> {
        throw UnsupportedOperationException()
    }
*/
    override fun <T : TableSchema<P>, P, C> AbstractColumn<C, T, out Any?>.get(id: Id<P, T>): C {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(this.fullName, "1"))!!
        return getColumnObject(doc, this) as C
    }
    override fun <T : AbstractTableSchema, C> iterator(q: Query<C, T>): Iterator<C> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(q.op)
        val fields = BasicDBObject()
        for (field in q.fields) {
            fields.append(field.fullName, "1")
        }
        val cursor = collection.find(query, fields)!!
        val results = ArrayList<C>()
        try {
            while(cursor.hasNext()) {
                val doc = cursor.next()
                results.add(when (q.fields.size) {
                    1 -> getColumnObject(doc, q.fields[0]) as C
                    2 -> Pair(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1])) as C
                    3 -> Triple(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1]), getColumnObject(doc, q.fields[2])) as C
                    4 -> Quadruple(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1]),
                            getColumnObject(doc, q.fields[2]), getColumnObject(doc, q.fields[3])) as C
                    5 -> Quintuple(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1]),
                            getColumnObject(doc, q.fields[2]), getColumnObject(doc, q.fields[3]),
                            getColumnObject(doc, q.fields[4])) as C
                    6 -> Sextuple(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1]),
                            getColumnObject(doc, q.fields[2]), getColumnObject(doc, q.fields[3]),
                            getColumnObject(doc, q.fields[4]), getColumnObject(doc, q.fields[5])) as C
                    7 -> Septuple(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1]),
                            getColumnObject(doc, q.fields[2]), getColumnObject(doc, q.fields[3]),
                            getColumnObject(doc, q.fields[4]), getColumnObject(doc, q.fields[5]),
                            getColumnObject(doc, q.fields[6])) as C
                    8 -> Octuple(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1]),
                            getColumnObject(doc, q.fields[2]), getColumnObject(doc, q.fields[3]),
                            getColumnObject(doc, q.fields[4]), getColumnObject(doc, q.fields[5]),
                            getColumnObject(doc, q.fields[6]), getColumnObject(doc, q.fields[7])) as C
                    9 -> Nonuple(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1]),
                            getColumnObject(doc, q.fields[2]), getColumnObject(doc, q.fields[3]),
                            getColumnObject(doc, q.fields[4]), getColumnObject(doc, q.fields[5]),
                            getColumnObject(doc, q.fields[6]), getColumnObject(doc, q.fields[7]),
                            getColumnObject(doc, q.fields[8])) as C
                    10 -> Decuple(getColumnObject(doc, q.fields[0]), getColumnObject(doc, q.fields[1]),
                            getColumnObject(doc, q.fields[2]), getColumnObject(doc, q.fields[3]),
                            getColumnObject(doc, q.fields[4]), getColumnObject(doc, q.fields[5]),
                            getColumnObject(doc, q.fields[6]), getColumnObject(doc, q.fields[7]),
                            getColumnObject(doc, q.fields[8]), getColumnObject(doc, q.fields[9])) as C
                    else -> throw UnsupportedOperationException()
                } )
            }
        } finally {
            cursor.close();
        }
        return results.iterator()
    }
    override fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.get(id: Id<P, T>): Pair<A, B> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1"))!!
        return Pair(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B)
    }
    override fun <T : TableSchema<P>, P, A, B, C> Template3<T, A, B, C>.get(id: Id<P, T>): Triple<A, B, C> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1")!!.append(c.fullName, "1"))!!
        return Triple(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B, getColumnObject(doc, c) as C)
    }
    override fun <T : TableSchema<P>, P, A, B, C, D> Template4<T, A, B, C, D>.get(id: Id<P, T>): Quadruple<A, B, C, D> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1")!!
                .append(c.fullName, "1")!!.append(d.fullName, "1"))!!
        return Quadruple(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B, getColumnObject(doc, c) as C, getColumnObject(doc, d) as D)
    }
    override fun <T : TableSchema<P>, P, A, B, C, D, E> Template5<T, A, B, C, D, E>.get(id: Id<P, T>): Quintuple<A, B, C, D, E> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1")!!
                .append(c.fullName, "1")!!.append(d.fullName, "1")!!.append(e.fullName, "1"))!!
        return Quintuple(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B, getColumnObject(doc, c) as C,
                getColumnObject(doc, d) as D, getColumnObject(doc, e) as E)
    }
    override fun <T : TableSchema<P>, P, A, B, C, D, E, F> Template6<T, A, B, C, D, E, F>.get(id: Id<P, T>): Sextuple<A, B, C, D, E, F> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1")!!
                .append(c.fullName, "1")!!.append(d.fullName, "1")!!.append(e.fullName, "1")!!.append(f.fullName, "1"))!!
        return Sextuple(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B, getColumnObject(doc, c) as C,
                getColumnObject(doc, d) as D, getColumnObject(doc, e) as E, getColumnObject(doc, f) as F)
    }
    override fun <T : TableSchema<P>, P, A, B, C, D, E, F, G> Template7<T, A, B, C, D, E, F, G>.get(id: Id<P, T>): Septuple<A, B, C, D, E, F, G> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1")!!
                .append(c.fullName, "1")!!.append(d.fullName, "1")!!.append(e.fullName, "1")!!.append(f.fullName, "1")!!
                .append(g.fullName, "1"))!!
        return Septuple(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B, getColumnObject(doc, c) as C,
                getColumnObject(doc, d) as D, getColumnObject(doc, e) as E, getColumnObject(doc, f) as F, getColumnObject(doc, g) as G)
    }
    override fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H> Template8<T, A, B, C, D, E, F, G, H>.get(id: Id<P, T>): Octuple<A, B, C, D, E, F, G, H> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1")!!
                .append(c.fullName, "1")!!.append(d.fullName, "1")!!.append(e.fullName, "1")!!.append(f.fullName, "1")!!
                .append(g.fullName, "1")!!.append(h.fullName, "1"))!!
        return Octuple(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B, getColumnObject(doc, c) as C,
                getColumnObject(doc, d) as D, getColumnObject(doc, e) as E, getColumnObject(doc, f) as F,
                getColumnObject(doc, g) as G, getColumnObject(doc, h) as H)
    }
    override fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, I> Template9<T, A, B, C, D, E, F, G, H, I>.get(id: Id<P, T>): Nonuple<A, B, C, D, E, F, G, H, I> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1")!!
                .append(c.fullName, "1")!!.append(d.fullName, "1")!!.append(e.fullName, "1")!!.append(f.fullName, "1")!!
                .append(g.fullName, "1")!!.append(h.fullName, "1")!!.append(j.fullName, "1"))!!
        return Nonuple(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B, getColumnObject(doc, c) as C,
                getColumnObject(doc, d) as D, getColumnObject(doc, e) as E, getColumnObject(doc, f) as F,
                getColumnObject(doc, g) as G, getColumnObject(doc, h) as H, getColumnObject(doc, j) as I)
    }
    override fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, I, J> Template10<T, A, B, C, D, E, F, G, H, I, J>.get(id: Id<P, T>): Decuple<A, B, C, D, E, F, G, H, I, J> {
        val table = Schema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.Id equal id)
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1")!!
                .append(c.fullName, "1")!!.append(d.fullName, "1")!!.append(e.fullName, "1")!!.append(f.fullName, "1")!!
                .append(g.fullName, "1")!!.append(h.fullName, "1")!!.append(i.fullName, "1")!!.append(j.fullName, "1"))!!
        return Decuple(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B, getColumnObject(doc, c) as C,
                getColumnObject(doc, d) as D, getColumnObject(doc, e) as E, getColumnObject(doc, f) as F,
                getColumnObject(doc, g) as G, getColumnObject(doc, h) as H, getColumnObject(doc, i) as I, getColumnObject(doc, j) as J)
    }

    private fun getColumnObject(doc: DBObject, column: AbstractColumn<*, *, *>): Any? {
        val columnObject = parse(doc, column.fullName.split("\\."))
        return if (column.columnType.primitive) {
            columnObject
        } else if (!column.columnType.custom && column.columnType.set) {
            (columnObject as BasicDBList).toSet()
        } else if (!column.columnType.custom && column.columnType.list) {
            (columnObject as BasicDBList).toList()
        } else if (column.columnType.custom && column.columnType.list) {
            (columnObject as BasicDBList).map { getObject(it as DBObject, column as ListColumn<Any?, out Schema>) }
        } else if (column.columnType.custom && column.columnType.set) {
            (columnObject as BasicDBList).map { getObject(it as DBObject, column as ListColumn<Any?, out Schema>) }.toSet()
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

    override fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Collection<C>, C> Query1<T, A, CC>.add(c: C) {
        // TODO TODO TODO
        update(array(Pair(a, listOf(c))), op!!, "\$pushAll")
    }
    override fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Collection<C>, C> Query1<T, A, CC>.delete(c: A.() -> Op) {
        val cOp = a.c()
        val collection = db.getCollection(Schema.current<Schema>().name)!!
        collection.update(getQuery(op), BasicDBObject().append("\$pull", BasicDBObject().append(a.fullName, getQuery(cOp, a.fullName)))!!)
    }

    override fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Set<C>, C> Query1<T, A, CC>.delete(c: C) {
        val collection = db.getCollection(Schema.current<Schema>().name)!!
        val fields = BasicDBObject().append("\$pull", BasicDBObject().append(a.fullName, getDBValue(c, a)))!!
        collection.update(getQuery(op), fields)
    }

    override fun <T : AbstractTableSchema, A: AbstractColumn<Int, T, Int>> Query1<T, A, Int>.add(c: Int): Int {
        throw UnsupportedOperationException()
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
    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }

    // TODO TODO TODO
    /*fun <T: DocumentSchema<P, V>, P, V> T.aggregate (body: AggregateBody<T>.() -> Unit) {

    }

    class AggregateBody<T: PolymorphicSchema<*, *>>(val t: T) {
        fun <A> group (a: T.() -> A): Group1<A> {
            return Group1()
        }
    }

    fun <T: PolymorphicSchema<*, *>> AggregateBody<T>.avg(column: AbstractColumn<*, T, Int>): AbstractColumn<*, T, Int> {
        return null as AbstractColumn<*, T, Int>
    }

    class Group1<A> {
        fun <B> group (b: A.() -> B): Group2<B> {
            return Group2()
        }
    }

    class Group2<A> {

    }*/
}