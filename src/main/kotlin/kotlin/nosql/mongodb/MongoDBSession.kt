package kotlin.nosql.mongodb

import kotlin.nosql.Session
import com.mongodb.DB
import kotlin.nosql.TableSchema
import kotlin.nosql.DocumentSchema
import kotlin.nosql.AbstractSchema
import kotlin.nosql.AbstractColumn
import kotlin.nosql.Op
import kotlin.nosql.Query1
import kotlin.nosql.PKTableSchema
import kotlin.nosql.Template2
import kotlin.nosql.Query2
import kotlin.nosql.RangeQuery
import kotlin.nosql.KeyValueSchema
import com.mongodb.BasicDBObject
import java.lang.reflect.Field
import java.util.ArrayList
import java.util.HashMap
import kotlin.nosql.PKColumn
import kotlin.nosql.util.getAllFields
import kotlin.nosql.util.getAllFieldsMap
import com.mongodb.DBObject
import kotlin.nosql.ColumnType
import kotlin.nosql.NullableColumn
import java.util.Arrays
import kotlin.nosql.Column

class MongoDBSession(val db: DB) : Session() {
    override fun <T : TableSchema> T.create() {
        throw UnsupportedOperationException()
    }

    override fun <T : TableSchema> T.drop() {
        throw UnsupportedOperationException()
    }

    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: () -> V): P {
        val collection = db.getCollection(this.name)!!
        val doc = getDBObject(v(), this)
        doc.set("class", valueClass.getName())
        collection.insert(doc)
        return doc.get("_id").toString() as P
    }

    private fun getDBObject(o: Any, schema: Any): BasicDBObject {
        val doc = BasicDBObject()
        val javaClass = o.javaClass
        val fields = getAllFields(javaClass)
        val schemaClass = schema.javaClass
        val schemaFields = getAllFieldsMap(schemaClass)
        for (field in fields) {
            val schemaField = schemaFields.get(field.getName()!!.toLowerCase())
            if (schemaField != null && javaClass<AbstractColumn<Any?, AbstractSchema, Any?>>().isAssignableFrom(schemaField.getType()!!)) {
                field.setAccessible(true)
                schemaField.setAccessible(true)
                val column = schemaField.get(schema) as AbstractColumn<out Any, out AbstractSchema, out Any>
                val value = field.get(o)
                if (value != null) {
                    // TODO TODO TODO
                    when (value) {
                        is Int -> doc.append(column.name, value)
                        is String -> doc.append(column.name, value)
                        else -> doc.append(column.name, getDBObject(value, column))
                    }
                }
            }
        }
        return doc
    }

    override fun <T : DocumentSchema<P, C>, P, C> T.filter(op: T.() -> Op): Iterator<C> {
        val collection = db.getCollection(this.name)!!
        val query = BasicDBObject()
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

    private fun <T: DocumentSchema<P, V>, P, V> getObject(doc: DBObject, schema: T): V {
        val valueInstance = newInstance(Class.forName(doc.get("class") as String))
        val schemaClass = schema.javaClass
        val schemaFields = getAllFields(schemaClass as Class<in Any?>)
        val valueFields = getAllFieldsMap(valueInstance.javaClass as Class<in Any?>)
        for (schemaField in schemaFields) {
            if (javaClass<AbstractColumn<Any?, T, Any?>>().isAssignableFrom(schemaField.getType()!!)) {
                val valueField = valueFields.get(if (schemaField.getName()!!.equals("pk")) "id" else schemaField.getName()!!.toLowerCase())
                if (valueField != null) {
                    schemaField.setAccessible(true)
                    valueField.setAccessible(true)
                    val column = schemaField.get(schema) as AbstractColumn<Any?, T, *>
                    val columnValue = when (column.columnType) {
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

    private fun <C> getObject(doc: DBObject, column: Column<C, *>): C {
        val valueInstance = newInstance(column.valueClass)
        val schemaClass = column.javaClass
        val columnFields = schemaClass.getDeclaredFields()
        val valueFields = getAllFieldsMap(valueInstance.javaClass as Class<in Any?>)
        for (columnField in columnFields) {
            if (javaClass<AbstractColumn<out Any?, out AbstractSchema, out Any?>>().isAssignableFrom(columnField.getType()!!)) {
                val valueField = valueFields.get(columnField.getName()!!.toLowerCase())
                if (valueField != null) {
                    columnField.setAccessible(true)
                    valueField.setAccessible(true)
                    val column = columnField.get(column) as AbstractColumn<Any?, *, *>
                    val columnValue = when (column.columnType) {
                        ColumnType.INTEGER, ColumnType.STRING -> doc.get(column.name)
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
        return valueInstance as C
    }

    override fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<out Any?, T, out Any?>, Any?>>) {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractSchema> delete(table: T, op: Op) {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, C> Query1<T, C>.set(c: () -> C) {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, C> AbstractColumn<C, T, out Any?>.forEach(statement: (C) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, C> AbstractColumn<C, T, out Any?>.iterator(): Iterator<C> {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, C, M> AbstractColumn<C, T, out Any?>.map(statement: (C) -> M): List<M> {
        throw UnsupportedOperationException()
    }
    override fun <T : PKTableSchema<P>, P, C> AbstractColumn<C, T, out Any?>.get(id: () -> P): C {
        throw UnsupportedOperationException()
    }
    override fun <T : PKTableSchema<P>, P, A, B> Template2<T, A, B>.get(id: () -> P): Pair<A, B> {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M> {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, C> RangeQuery<T, C>.forEach(st: (C) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, C, CC : Collection<Any?>> Query1<T, CC>.add(c: () -> C) {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema> Query1<T, Int>.add(c: () -> Int): Int {
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
    override fun <T : TableSchema> AbstractColumn<Int, T, out Any?>.add(c: () -> Int): Int {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }

}