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

class RedisDBSession(val jedis: Jedis): Session, KeyValueSchemaOperations, DocumentSchemaOperations {
    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T> {
        throw UnsupportedOperationException()
    }
    override fun <T: DocumentSchema<P, C>, P, C> T.find(query: T.() -> Query): DocumentSchemaQueryWrapper<T, P, C> {
        throw UnsupportedOperationException()
    }
    override fun <T : DocumentSchema<P, C>, P, C> find(params: DocumentSchemaQueryParams<T, P, C>): Iterator<C> {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractSchema> T.create() {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractSchema> T.drop() {
        throw UnsupportedOperationException()
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
    override fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, out Any?>): Int {
        throw UnsupportedOperationException()
    }
    override fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, out Any?>, v: C) {
        throw UnsupportedOperationException()
    }
}