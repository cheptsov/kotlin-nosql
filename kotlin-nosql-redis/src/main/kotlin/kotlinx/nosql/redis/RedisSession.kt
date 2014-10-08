package kotlinx.nosql.redis

import redis.clients.jedis.Jedis
import kotlinx.nosql.Session
import kotlinx.nosql.AbstractTableSchema
import kotlinx.nosql.DocumentSchema
import kotlinx.nosql.Id
import kotlinx.nosql.AbstractSchema
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.Query
import kotlinx.nosql.TableSchema
import kotlinx.nosql.KeyValueSchema
import kotlinx.nosql.AbstractIndex
import kotlinx.nosql.KeyValueSchemaOperations
import kotlinx.nosql.DocumentSchemaQueryParams
import kotlinx.nosql.TableSchemaProjectionQueryParams
import kotlinx.nosql.DocumentSchemaOperations
import kotlinx.nosql.query.NoQuery
import kotlinx.nosql.DocumentSchemaQueryWrapper
import kotlinx.nosql.util.getAllFields
import java.util.HashMap
import kotlinx.nosql.util.getAllFieldsMap
import kotlinx.nosql.util.isColumn
import kotlinx.nosql.util.asColumn
import org.joda.time.DateTime
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import java.util.ArrayList

class RedisSession(val jedis: Jedis): Session, KeyValueSchemaOperations, DocumentSchemaOperations {
    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T> {
      val doc = getObjectMap(v, this)
      if (discriminator != null) {
        var dominatorValue: Any? = null
        for (entry in kotlinx.nosql.DocumentSchema.discriminatorClasses.entrySet()) {
          if (entry.value.equals(v.javaClass)) {
            dominatorValue = entry.key.value
          }
        }
        doc.set(this.discriminator.column.name, dominatorValue!!)
      }
      val id = doc.filter {
        // TODO: fix me
        it.key.equals("id")
      }.values().single().toString()

      for ((key, value) in doc) {
        // TODO: No support for embedded documents
        jedis.hset(schemaName + ":" + id, key.toString(), value.toString())
      }
      return Id<P, T>(id as P)
    }
    override fun <T: DocumentSchema<P, C>, P, C> T.find(query: T.() -> Query): DocumentSchemaQueryWrapper<T, P, C> {
        throw UnsupportedOperationException()
    }
    override fun <T : DocumentSchema<P, C>, P, C> find(params: DocumentSchemaQueryParams<T, P, C>): Iterator<C> {
        throw UnsupportedOperationException()
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
    override fun <T : KeyValueSchema, R : TableSchema<P>, P> T.next(c: T.() -> AbstractColumn<Id<P, R>, T, *>): Id<P, R> {
      throw UnsupportedOperationException()
    }
    override fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, out Any?>, v: C) {
        throw UnsupportedOperationException()
    }

    class ObjectId(val id: String) {
      override fun toString(): String {
        return id
      }
    }

    private fun getObjectMap(o: Any, schema: Any): MutableMap<Any, Any> {
      val doc = HashMap<Any, Any>()
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
              doc.set(column.name, if (column.columnType.id) ObjectId(value.toString()) else value.toString())
            } else if (column.columnType.iterable) {
              val list = ArrayList<Any>()
              for (v in (value as Iterable<*>)) {
                list.add(if (column.columnType.custom) getObjectMap(v!!, column) else
                  if (column.columnType.id) ObjectId(v!!.toString()) else v!!)
              }
              doc.set(column.name, list)
            } else
              doc.set(column.name, getObjectMap(value, column))
          }
        }
      }
      return doc
    }

}
