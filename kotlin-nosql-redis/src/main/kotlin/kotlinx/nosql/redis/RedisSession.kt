package kotlinx.nosql.redis

import redis.clients.jedis.Jedis
import kotlinx.nosql.Session
import kotlinx.nosql.DocumentSchema
import kotlinx.nosql.Id
import kotlinx.nosql.AbstractSchema
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.Query
import kotlinx.nosql.TableSchema
import kotlinx.nosql.KeyValueSchema
import kotlinx.nosql.KeyValueSchemaOperations
import kotlinx.nosql.util.getAllFields
import java.util.HashMap
import kotlinx.nosql.util.getAllFieldsMap
import kotlinx.nosql.util.isColumn
import kotlinx.nosql.util.asColumn
import org.joda.time.DateTime
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import java.util.ArrayList
import kotlinx.nosql.KeyValueDocumentSchemaOperations
import kotlinx.nosql.util.newInstance
import kotlinx.nosql.ColumnType
import kotlinx.nosql.AbstractNullableColumn
import kotlinx.nosql.IdListColumn
import kotlinx.nosql.IdSetColumn
import java.util.HashSet

class RedisSession(val jedis: Jedis) : Session, KeyValueSchemaOperations, KeyValueDocumentSchemaOperations {
    override fun <T : Number> incr(schema: KeyValueSchema, column: AbstractColumn<out Any?, out AbstractSchema, T>, value: T): T {
        return jedis.incrBy(schema.schemaName + ":" + column.name, value.toLong())!!.toInt() as T
    }
    /*override fun <T : Number> incr(schema: AbstractSchema, column: AbstractColumn<out Any?, out AbstractSchema, T>, value: T, op: Query): T {
        throw UnsupportedOperationException()
    }*/
    /**
     * TODO: Replace with AbstractSchema.createInstance()
     */
    fun <T : kotlinx.nosql.DocumentSchema<P, V>, P, V> T.createInstance(reader: (column: AbstractColumn<*, *, *>) -> Any): V {
        return if (this.discriminator != null) {
            var instance: Any? = null
            val discriminatorValue = reader(this.discriminator.column)
            for (discriminator in kotlinx.nosql.DocumentSchema.tableDiscriminators.get(this.schemaName)!!) {
                if (discriminator.value!!.equals(discriminatorValue)) {
                    instance = newInstance(kotlinx.nosql.DocumentSchema.discriminatorClasses.get(discriminator)!!)
                    break
                }
            }
            instance!! as V
        } else {
            newInstance(this.valueClass) as V
        }
    }

    public fun <T : kotlinx.nosql.DocumentSchema<P, V>, P, V> T.getSchema(reader: (column: AbstractColumn<*, *, *>) -> Any): AbstractSchema {
        var s: AbstractSchema? = null
        if (this.discriminator != null) {
            val discriminatorValue = reader(this.discriminator.column)
            for (discriminator in kotlinx.nosql.DocumentSchema.tableDiscriminators.get(this.schemaName)!!) {
                if (discriminator.value!!.equals(discriminatorValue)) {
                    s = kotlinx.nosql.DocumentSchema.discriminatorSchemas.get(discriminator)!!
                    break
                }
            }
        } else {
            s = this
        }
        return s!!
    }

    public fun set(instance: Any, name: String, column: AbstractColumn<*, *, *>, value: Any?) {
        val field = instance.javaClass.getDeclaredField(if (name.equals("pk")) "id" else name)
        field.setAccessible(true)
        if (column is AbstractNullableColumn && value == null)
            throw NullPointerException()
        field.set(instance, value)
    }

    public val AbstractSchema.columns: Map<String, AbstractColumn<*, *, *>>
        get() {
            return getAllFields(javaClass).filter { it.isColumn }.map { Pair(it.getName()!!, it.asColumn(this)) }.toMap()
        }

    internal override fun <T : DocumentSchema<P, C>, P, C> T.get(id: Id<P, T>): C? {
        val map = jedis.hgetAll(schemaName + ":" + id.value)!!
        val schema = getSchema {
            map.get(it.name)!!
        }
        val instance = createInstance {
            map.get(it.name)!!
        }
        schema.columns.forEach { entry ->
            val (name, column) = entry
            if (column.columnType.primitive) {
                val value = map.get(column.name)
                if (value != null) {
                    set(instance, name.toLowerCase(), column, when (column.columnType) {
                        ColumnType.INTEGER -> value.toInt()
                        ColumnType.BOOLEAN -> value.toBoolean()
                        ColumnType.FLOAT -> value.toFloat()
                        ColumnType.DOUBLE -> value.toDouble()
                        ColumnType.LONG -> value.toLong()
                        ColumnType.SHORT -> value.toShort()
                        ColumnType.DATE -> LocalDate(value)
                        ColumnType.TIME -> LocalTime(value)
                        ColumnType.DATE_TIME -> DateTime(value)
                        ColumnType.STRING -> value
                        ColumnType.PRIMARY_ID -> when (column.valueClass.getName()) {
                            "int" -> Id<Any, TableSchema<Any>>(value.toInt())
                            "java.lang.String" -> Id<Any, TableSchema<Any>>(value.toString())
                            else -> throw UnsupportedOperationException()
                        }
                        else -> throw UnsupportedOperationException("Unsupported column type: " + column.columnType)
                    })
                } else {
                    if (column !is AbstractNullableColumn) {
                        throw NullPointerException()
                    }
                }
            } else if (column.columnType.list) {
                val list = jedis.lrange(schemaName + ":" + id.value + ":" + column.name, 0, -1)
                if (list != null) {
                    val value = list.map {
                        when (column.columnType) {
                            ColumnType.INTEGER_LIST -> it.toInt()
                            ColumnType.ID_LIST -> Id<Any, TableSchema<Any>>(when ((column as IdListColumn<*, *, *>).refSchema.pk.valueClass.getName()) {
                                "int" -> it.toInt()
                                "java.lang.String" -> it.toString()
                                else -> throw UnsupportedOperationException()
                            })
                            else -> it
                        }
                    }
                    set(instance, name, column, value)
                }
            } else if (column.columnType.set) {
                val set = jedis.zrange(schemaName + ":" + id.value + ":" + column.name, 0, -1)
                if (set != null) {
                    val value = set.map {
                        when (column.columnType) {
                            ColumnType.INTEGER_SET -> it.toInt()
                            ColumnType.ID_LIST -> Id<Any, TableSchema<Any>>(when ((column as IdListColumn<*, *, *>).refSchema.pk.valueClass.getName()) {
                                "int" -> it.toInt()
                                "java.lang.String" -> it.toString()
                                else -> throw UnsupportedOperationException()
                            })
                            else -> it
                        }
                    }.toSet()
                    set(instance, name, column, value)
                }
            }
        }
        return instance
    }

    /*
     * TODO: Refactor
     */
    fun <T : kotlinx.nosql.DocumentSchema<P, V>, P, V> getObject(map: Map<String, String>, schema: T): V {
        var s: AbstractSchema? = null
        val valueInstance: Any = if (schema is kotlinx.nosql.DocumentSchema<*, *> && schema.discriminator != null) {
            var instance: Any? = null
            val discriminatorValue = map.get(schema.discriminator.column.name)
            for (discriminator in kotlinx.nosql.DocumentSchema.tableDiscriminators.get(schema.schemaName)!!) {
                if (discriminator.value!!.equals(discriminatorValue)) {
                    instance = newInstance(kotlinx.nosql.DocumentSchema.discriminatorClasses.get(discriminator)!!)
                    s = kotlinx.nosql.DocumentSchema.discriminatorSchemas.get(discriminator)!!
                    break
                }
            }
            instance!!
        } else {
            s = schema
            newInstance(schema.valueClass)
        }
        val schemaClass = s!!.javaClass
        val schemaFields = getAllFields(schemaClass as Class<in Any?>)
        val valueFields = getAllFieldsMap(valueInstance.javaClass as Class<in Any?>)
        for (schemaField in schemaFields) {
            if (javaClass<AbstractColumn<Any?, T, Any?>>().isAssignableFrom(schemaField.getType()!!)) {
                val valueField = valueFields.get(if (schemaField.getName()!!.equals("pk")) "id" else schemaField.getName()!!.toLowerCase())
                if (valueField != null) {
                    schemaField.setAccessible(true)
                    valueField.setAccessible(true)
                    val column = schemaField.asColumn(s!!)
                    val value = map.get(column.name)
                    val columnValue: Any? = if (value == null) {
                        null
                    } else if (column.columnType.id && !column.columnType.iterable)
                        Id<P, T>(value.toString() as P)
                    else if (column.columnType.primitive) {
                        when (column.columnType) {
                            ColumnType.DATE -> LocalDate(value.toString())
                            ColumnType.TIME -> LocalTime(value.toString())
                            ColumnType.DATE_TIME -> DateTime(value.toString())
                            else -> map.get(column.name)
                        }
                    } else {
                        throw UnsupportedOperationException()
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

    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T> {
        val doc = getObjectMap(v, this)
        if (discriminator != null) {
            var dominatorValue: Any? = null
            for (entry in kotlinx.nosql.DocumentSchema.discriminatorClasses.entrySet()) {
                if (entry.value.equals(v.javaClass)) {
                    dominatorValue = entry.key.value
                }
            }
            doc.set(this.discriminator.column, dominatorValue!!)
        }
        // TODO: Extract a common method
        val id = doc.filter {
            it.key.columnType == ColumnType.PRIMARY_ID
        }.values().single().toString()

        for ((column, value) in doc) {
            // TODO: No support for embedded documents
            if (column.columnType.primitive) {
                // TODO: Replace toString with correct serialization
                jedis.hset(schemaName + ":" + id, column.name, value.toString())
            } else if (column.columnType.list) {
                jedis.del(schemaName + ":" + id + ":" + column.name)
                for (v in value as List<*>) {
                    jedis.rpush(schemaName + ":" + id + ":" + column.name, v.toString())
                }
            } else if (column.columnType.set) {
                jedis.del(schemaName + ":" + id + ":" + column.name)
                for (v in value as Set<*>) {
                    jedis.sadd(schemaName + ":" + id + ":" + column.name, v.toString())
                }
            }
        }
        return Id<P, T>(id as P)
    }

    override fun <T : AbstractSchema> T.create() {
        // Do nothing
    }

    override fun <T : AbstractSchema> T.drop() {
        jedis.keys(schemaName + ":*")!!.forEach {
            jedis.del(it)
        }
    }

    override fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<out Any?, T, out Any?>, Any?>>) {
        throw UnsupportedOperationException()
    }

    override fun <T : AbstractSchema> delete(table: T, op: Query): Int {
        throw UnsupportedOperationException()
    }

    override fun update(schema: AbstractSchema, columnValues: Array<Pair<AbstractColumn<out Any?, out AbstractSchema, out Any?>, Any?>>, op: Query): Int {
        throw UnsupportedOperationException()
    }

    override fun <T> addAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, out AbstractSchema, out Any?>, values: Collection<T>, op: Query): Int {
        throw UnsupportedOperationException()
    }

    override fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, out AbstractSchema, out Any?>, values: Collection<T>, op: Query): Int {
        throw UnsupportedOperationException()
    }

    override fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, out AbstractSchema, out Any?>, removeOp: Query, op: Query): Int {
        throw UnsupportedOperationException()
    }

    override fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, out Any?>): C {
        throw UnsupportedOperationException()
    }
    /*
        override fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, out Any?>): Int {
            throw UnsupportedOperationException()
        }
    */

    override fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, out Any?>, v: C) {
        throw UnsupportedOperationException()
    }


    private fun getObjectMap(o: Any, schema: Any): MutableMap<AbstractColumn<*, *, *>, Any?> {
        val doc = HashMap<AbstractColumn<*, *, *>, Any?>()
        val javaClass = o.javaClass
        val fields = getAllFields(javaClass)
        var sc: Class<out Any?>? = null
        var s: AbstractSchema? = null
        if (schema is kotlinx.nosql.DocumentSchema<*, *> && schema.discriminator != null) {
            for (entry in kotlinx.nosql.DocumentSchema.discriminatorClasses.entrySet()) {
                if (entry.value.equals(o.javaClass)) {
                    sc = kotlinx.nosql.DocumentSchema.discriminatorSchemaClasses.get(entry.key)!!
                    s = kotlinx.nosql.DocumentSchema.discriminatorSchemas.get(entry.key)!!
                }
            }
        }
        val schemaClass: Class<out Any?> = if (schema is kotlinx.nosql.DocumentSchema<*, *> && schema.discriminator != null) sc!! else schema.javaClass
        val objectSchema: Any = if (schema is kotlinx.nosql.DocumentSchema<*, *> && schema.discriminator != null) s!! else schema
        val schemaFields = getAllFieldsMap(schemaClass as Class<in Any>, { f -> f.isColumn })
        for (field in fields) {
            val schemaField = schemaFields.get(if (field.getName()!!.equals("id")) "pk" else field.getName()!!.toLowerCase())
            if (schemaField != null && schemaField.isColumn) {
                field.setAccessible(true)
                schemaField.setAccessible(true)
                val column = schemaField.asColumn(objectSchema)
                val value = field.get(o)
                if (value != null) {
                    if (column.columnType.primitive) {
                        doc.set(column, value)
                    } else if (column.columnType.list) {
                        val list = ArrayList<Any>()
                        for (v in (value as Iterable<*>)) {
                            list.add(if (column.columnType.custom) getObjectMap(v!!, column) else v!!)
                        }
                        doc.set(column, list)
                    } else if (column.columnType.set) {
                        val set = HashSet<Any>()
                        for (v in (value as Iterable<*>)) {
                            set.add(if (column.columnType.custom) getObjectMap(v!!, column) else v!!)
                        }
                        doc.set(column, set)
                    } else
                        doc.set(column, getObjectMap(value, column))
                }
            }
        }
        return doc
    }

}
