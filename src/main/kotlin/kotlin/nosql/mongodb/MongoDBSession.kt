package kotlin.nosql.mongodb

import kotlin.nosql.Database
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

class MongoDBSession(val db: DB) : Session() {
    override fun <T : TableSchema> T.create() {
        throw UnsupportedOperationException()
    }
    override fun <T : TableSchema> T.drop() {
        throw UnsupportedOperationException()
    }
    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: () -> V): P {
        val collection = db.getCollection(this.name)!!
        val dbObject = toDBObject(v(), this)
        collection.insert(dbObject)
        return dbObject.get("_id").toString() as P
    }
    private fun toDBObject(o: Any, schema: Any): BasicDBObject {
        val dbObject = BasicDBObject()
        val javaClass = o.javaClass
        val fields = getAllFields(ArrayList(), javaClass)
        val schemaClass = schema.javaClass
        val schemaFields = getAllFields(ArrayList(), schemaClass)
        for (field in fields) {
            for (schemaField in schemaFields) {
                if (schemaField.getName()!!.equalsIgnoreCase(field.getName()!!) &&
                javaClass<AbstractColumn<Any?, AbstractSchema, Any?>>().isAssignableFrom(schemaField.getType()!!)) {
                    field.setAccessible(true)
                    schemaField.setAccessible(true)
                    val column = schemaField.get(schema) as AbstractColumn<out Any, out AbstractSchema, out Any>
                    val value = field.get(o)
                    if (value != null) {
                        // TODO TODO TODO
                        when (value) {
                            is Int -> dbObject.append(column.name, value)
                            is String -> dbObject.append(column.name, value)
                            else -> dbObject.append(column.name, toDBObject(value, column))
                        }
                    }
                    break
                }
            }
        }
        return dbObject
    }

    fun getAllFields(fields: MutableList<Field> , _type: Class<in Any>): List<Field> {
        for (field in _type.getDeclaredFields()) {
            fields.add(field)
        }

        if (_type.getSuperclass() != null) {
            return getAllFields(fields, _type.getSuperclass()!!);
        } else {
            return fields
        }
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
    override fun <T : DocumentSchema<P, C>, P, C> T.filter(op: T.() -> Op): Iterator<C> {
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