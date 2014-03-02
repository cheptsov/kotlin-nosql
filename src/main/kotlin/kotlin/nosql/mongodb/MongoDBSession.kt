package kotlin.nosql.mongodb

import kotlin.nosql.Session
import com.mongodb.DB
import kotlin.nosql.AbstractTableSchema
import kotlin.nosql.DocumentSchema
import kotlin.nosql.AbstractSchema
import kotlin.nosql.AbstractColumn
import kotlin.nosql.Op
import kotlin.nosql.Query1
import kotlin.nosql.TableSchema
import kotlin.nosql.Template2
import kotlin.nosql.Query2
import kotlin.nosql.RangeQuery
import kotlin.nosql.KeyValueSchema
import com.mongodb.BasicDBObject
import java.lang.reflect.Field
import java.util.ArrayList
import java.util.HashMap
import kotlin.nosql.PrimaryKeyColumn
import kotlin.nosql.util.*
import com.mongodb.DBObject
import kotlin.nosql.ColumnType
import kotlin.nosql.NullableColumn
import java.util.Arrays
import kotlin.nosql.Column
import kotlin.nosql.Discriminator
import kotlin.nosql.PolymorphicSchema
import kotlin.nosql.util.asColumn
import kotlin.nosql.EqualsOp
import kotlin.nosql.LiteralOp
import kotlin.nosql.AndOp
import kotlin.nosql.OrOp
import org.bson.types.ObjectId
import com.mongodb.BasicDBList
import kotlin.nosql.ColumnType.INTEGER_LIST
import kotlin.nosql.ListColumn

class MongoDBSession(val db: DB) : Session() {
    override fun <T : AbstractTableSchema> T.create() {
        throw UnsupportedOperationException()
    }

    override fun <T : AbstractTableSchema> T.drop() {
        val collection = db.getCollection(this.name)!!
        collection.remove(BasicDBObject())
    }

    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: () -> V): P {
        val collection = db.getCollection(this.name)!!
        val obj = v()
        val doc = getDBObject(obj, this)
        if (this is PolymorphicSchema<*, *>) {
            var dominatorValue: Any? = null
            for (entry in PolymorphicSchema.discriminatorClasses.entrySet()) {
                if (entry.value.equals(obj.javaClass)) {
                    dominatorValue = entry.key.value
                }
            }
            doc.set(this.discriminator.column.name, dominatorValue!!)
        }
        collection.insert(doc)
        return doc.get("_id").toString() as P
    }

    private fun getDBObject(o: Any, schema: Any): BasicDBObject {
        val doc = BasicDBObject()
        val javaClass = o.javaClass
        val fields = getAllFields(javaClass)
        var sc: Class<out Any?>? = null
        var s: AbstractSchema? = null
        if (schema is PolymorphicSchema<*, *>) {
            for (entry in PolymorphicSchema.discriminatorClasses.entrySet()) {
                if (entry.value.equals(o.javaClass)) {
                    sc = PolymorphicSchema.discriminatorSchemaClasses.get(entry.key)!!
                    s = PolymorphicSchema.discriminatorSchemas.get(entry.key)!!
                }
            }
        }
        val schemaClass: Class<out Any?> = if (schema is PolymorphicSchema<*, *>) sc!! else schema.javaClass
        val objectSchema: Any = if (schema is PolymorphicSchema<*, *>) s!! else schema
        val schemaFields = getAllFieldsMap(schemaClass as Class<in Any>)
        for (field in fields) {
            val schemaField = schemaFields.get(field.getName()!!.toLowerCase())
            if (schemaField != null && schemaField.isColumn) {
                field.setAccessible(true)
                schemaField.setAccessible(true)
                val column = schemaField.asColumn(objectSchema)
                val value = field.get(o)
                if (value != null) {
                    // TODO TODO TODO
                    when (value) {
                        is Int -> doc.append(column.name, value)
                        is String -> doc.append(column.name, value)
                        is Iterable<*> -> {
                            val list = BasicDBList()
                            for (v in value) {
                                list.add(when (column.columnType ) {
                                    ColumnType.INTEGER_LIST, ColumnType.STRING_LIST,
                                    ColumnType.INTEGER_SET, ColumnType.STRING_SET-> v!!
                                    ColumnType.CUSTOM_CLASS_SET, ColumnType.CUSTOM_CLASS_LIST-> getDBObject(v!!, column)
                                    else -> throw UnsupportedOperationException()
                                })
                            }
                            doc.append(column.name, list)
                        }
                        else -> doc.append(column.name, getDBObject(value, column))
                    }
                }
            }
        }
        return doc
    }

    override fun <T : DocumentSchema<P, C>, P, C> T.filter(op: T.() -> Op): Iterator<C> {
        val collection = db.getCollection(this.name)!!
        val query = getQuery(op())
        val cursor = collection.find(query)!!
        val docs = ArrayList<C>()
        try {
            while(cursor.hasNext()) {
                val doc = cursor.next()
                val obj = getObject(doc, this)
                docs.add(obj)
            }
            return docs.iterator()
        } finally {
            cursor.close();
        }
    }

    private fun getQuery(op: Op): BasicDBObject {
        val query = BasicDBObject()
        when (op) {
            is EqualsOp -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralOp) {
                        if (op.expr2.value is String || op.expr2.value is Int) {
                            if (op.expr1 is PrimaryKeyColumn<*, *>) {
                                query.append(op.expr1.fullName, ObjectId(op.expr2.value.toString()))
                            } else {
                                query.append(op.expr1.fullName, op.expr2.value)
                            }
                        } else {
                            throw UnsupportedOperationException()
                        }
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
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
        val valueInstance: Any = when (schema) {
            is PolymorphicSchema<*, *> -> {
                var instance: Any? = null
                val discriminatorValue = doc.get(schema.discriminator.column.name)
                for (discriminator in PolymorphicSchema.tableDiscriminators.get(schema.name)!!) {
                    if (discriminator.value.equals(discriminatorValue)) {
                        instance = newInstance(PolymorphicSchema.discriminatorClasses.get(discriminator)!!)
                        break
                    }
                }
                instance!!
            }
            else -> newInstance(schema.valueClass)
        }
        val schemaClass = schema.javaClass
        val schemaFields = getAllFields(schemaClass as Class<in Any?>)
        val valueFields = getAllFieldsMap(valueInstance.javaClass as Class<in Any?>)
        for (schemaField in schemaFields) {
            if (javaClass<AbstractColumn<Any?, T, Any?>>().isAssignableFrom(schemaField.getType()!!)) {
                val valueField = valueFields.get(if (schemaField.getName()!!.equals("pk")) "id" else schemaField.getName()!!.toLowerCase())
                if (valueField != null) {
                    schemaField.setAccessible(true)
                    valueField.setAccessible(true)
                    val column = schemaField.asColumn(schema)
                    val columnValue: Any? = when (column.columnType) {
                        ColumnType.INTEGER -> doc.get(column.name)
                        ColumnType.STRING -> doc.get(column.name)?.toString()
                        else -> {
                            getObject(doc.get(column.name) as DBObject, column as Column<Any?, T>)
                        }
                    }
                    if (columnValue != null || column is NullableColumn<*, *>) {
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
                    val columnValue = when (column.columnType) {
                        ColumnType.INTEGER, ColumnType.STRING -> doc.get(column.name)
                        ColumnType.INTEGER_LIST, ColumnType.STRING_LIST -> (doc.get(column.name) as BasicDBList).toList()
                        ColumnType.INTEGER_SET, ColumnType.STRING_SET -> (doc.get(column.name) as BasicDBList).toSet()
                        ColumnType.CUSTOM_CLASS_LIST -> {
                            val list = doc.get(column.name) as BasicDBList
                            list.map { getObject(it as DBObject, column as ListColumn<Any?, out AbstractSchema>) }
                        }
                        ColumnType.CUSTOM_CLASS_SET -> {
                            val list = doc.get(column.name) as BasicDBList
                            list.map { getObject(it as DBObject, column as ListColumn<Any?, out AbstractSchema>) }.toSet()
                        }
                        else -> {
                            getObject(doc.get(column.name) as DBObject, column as Column<Any?, out AbstractSchema>)
                        }
                    }
                    if (columnValue != null || column is NullableColumn<*, *>) {
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
    override fun <T : AbstractSchema> delete(table: T, op: Op) {
        val collection = db.getCollection(table.name)!!
        val query = getQuery(op)
        collection.remove(query)
    }
    override fun <T : AbstractTableSchema, C> Query1<T, C>.set(c: () -> C) {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, C> AbstractColumn<C, T, out Any?>.forEach(statement: (C) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, C> AbstractColumn<C, T, out Any?>.iterator(): Iterator<C> {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, C, M> AbstractColumn<C, T, out Any?>.map(statement: (C) -> M): List<M> {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema<P>, P, C> AbstractColumn<C, T, out Any?>.get(id: () -> P): C {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.get(id: () -> P): Pair<A, B> {
        val table = AbstractSchema.current<T>()
        val collection = db.getCollection(table.name)!!
        val query = getQuery(table.pk eq id())
        val doc = collection.findOne(query, BasicDBObject().append(a.fullName, "1")!!.append(b.fullName, "1"))!!
        return Pair(getColumnObject(doc, a) as A, getColumnObject(doc, b) as B)
    }

    private fun getColumnObject(doc: DBObject, column: AbstractColumn<*, *, *>): Any? {
        val columnObject = parse(doc, column.fullName.split("\\."))
        return when (columnObject) {
            is String, is Integer -> columnObject
            is BasicDBList -> when (column.columnType) {
                ColumnType.STRING_SET, ColumnType.INTEGER_SET -> columnObject.toSet()
                ColumnType.STRING_LIST, ColumnType.INTEGER_LIST -> columnObject.toList()
                ColumnType.CUSTOM_CLASS_LIST -> {
                    columnObject.map { getObject(it as DBObject, column as ListColumn<Any?, out AbstractSchema>) }
                }
                ColumnType.CUSTOM_CLASS_SET -> {
                    columnObject.map { getObject(it as DBObject, column as ListColumn<Any?, out AbstractSchema>) }.toSet()
                }
                else -> throw UnsupportedOperationException()
            }
            is DBObject -> getObject(columnObject, column)
            else -> throw UnsupportedOperationException()
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

    override fun <T : AbstractTableSchema, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M> {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, C> RangeQuery<T, C>.forEach(st: (C) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, C, CC : Collection<Any?>> Query1<T, CC>.add(c: () -> C) {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema> Query1<T, Int>.add(c: () -> Int): Int {
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
    override fun <T : AbstractTableSchema> AbstractColumn<Int, T, out Any?>.add(c: () -> Int): Int {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }

}