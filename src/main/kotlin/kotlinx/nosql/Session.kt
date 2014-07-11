package kotlinx.nosql

import java.util.ArrayList
import com.mongodb.BasicDBObject
import kotlinx.nosql.Session.TableSchemaProjectionQueryWrapper

val tableSchemaProjectionObservableThreadLocale = ThreadLocal<TableSchemaProjectionQueryWrapper<out TableSchema<*>, *, *>>()

abstract class Session () {
    abstract fun <T : AbstractTableSchema>T.create()

    abstract fun <T : AbstractTableSchema>T.drop()

    abstract fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T>

    abstract fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<*, T, *>, *>>)

    abstract fun <T : AbstractSchema> delete(table: T, op: Op): Int

    abstract fun update(schema: AbstractSchema, columnValues: Array<Pair<AbstractColumn<*, *, *>, *>>, op: Op): Int

    abstract fun <T> addAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Op): Int

    abstract fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Op): Int

    abstract fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, removeOp: Op, op: Op): Int

    class object {
        val threadLocale = ThreadLocal<Session>()

        fun current(): Session {
            return threadLocale.get()!!
        }
    }

    class DocumentSchemaQueryParams<T : DocumentSchema<P, C>, P, C>(val schema: T, val query: Op? = null,
                                      var skip: Int? = null, var take: Int? = null, var subscribed: Boolean = false)


    inner class DocumentSchemaQueryWrapper<T : DocumentSchema<P, C>, P, C>(val params: DocumentSchemaQueryParams<T, P, C>): Iterable<C> {
        override fun iterator(): Iterator<C> {
            return find(params)
        }

        fun skip(num: Int): DocumentSchemaQueryWrapper<T, P, C> {
            params.skip = num
            return this
        }

        fun take(num: Int): DocumentSchemaQueryWrapper<T, P, C> {
            params.take = num
            return this
        }

        fun remove(): Int {
            return delete(params.schema, params.query!!)
        }

        fun <X> projection(x: T.() -> X): X {
            val xx = params.schema.x()
            val projectionParams = TableSchemaProjectionQueryParams<TableSchema<Any?>, Any?, Any?>(params.schema as TableSchema<Any?>,
                    when (xx) {
                        is AbstractColumn<*, *, *> -> listOf(xx)
                        is Template2<*, *, *> -> listOf(xx.a, xx.b)
                        is Template3<*, *, *, *> -> listOf(xx.a, xx.b, xx.c)
                        is Template4<*, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d)
                        is Template5<*, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e)
                        is Template6<*, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f)
                        is Template7<*, *, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f, xx.g)
                        is Template8<*, *, *, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f, xx.g, xx.h)
                        is Template9<*, *, *, *, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f, xx.g, xx.h, xx.j)
                        is Template10<*, *, *, *, *, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f, xx.g, xx.h, xx.i, xx.j)
                        else -> throw UnsupportedOperationException()
                    }, params.query)
            tableSchemaProjectionObservableThreadLocale.set(TableSchemaProjectionQueryWrapper(projectionParams))
            return params.schema.x()
        }
    }

    class TableSchemaProjectionQueryParams<T : TableSchema<P>, P, V>(val table: T, val projection: List<AbstractColumn<*, *, *>>, val query: Op? = null,
                                                                var skip: Int? = null, var take: Int? = null)

    inner class TableSchemaProjectionQueryWrapper<T : TableSchema<P>, P, V>(val params: TableSchemaProjectionQueryParams<T, P, V>): Iterable<V> {
        override fun iterator(): Iterator<V> {
            return find(params)
        }

        fun skip(num: Int): TableSchemaProjectionQueryWrapper<T, P, V> {
            params.skip = num
            return this
        }

        fun take(num: Int): TableSchemaProjectionQueryWrapper<T, P, V> {
            params.take = num
            return this
        }
    }

    abstract fun <T : DocumentSchema<P, C>, P, C> find(params: DocumentSchemaQueryParams<T, P, C>): Iterator<C>

    abstract fun <T : TableSchema<P>, P, V> find(params: TableSchemaProjectionQueryParams<T, P, V>): Iterator<V>

    fun <T: DocumentSchema<P, C>, P, C> T.find(query: T.() -> Op = { NoOp }): DocumentSchemaQueryWrapper<T, P, C> {
        val params = DocumentSchemaQueryParams<T, P, C>(this, query())
        return DocumentSchemaQueryWrapper(params)
    }

    abstract fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, *>): C
    abstract fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, *>): Int
    abstract fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, *>, v: C)
}
