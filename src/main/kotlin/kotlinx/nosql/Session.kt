package kotlinx.nosql

import java.util.ArrayList
import rx.Observable
import rx.Observable.OnSubscribeFunc
import com.mongodb.BasicDBObject
import rx.subscriptions.Subscriptions
import rx.Subscription
import rx.Observer
import kotlinx.nosql.Session.TableSchemaProjectionQueryObservable

val tableSchemaProjectionObservableThreadLocale = ThreadLocal<TableSchemaProjectionQueryObservable<out TableSchema<*>, *, *>>()

abstract class Session () {
    abstract fun <T : AbstractTableSchema>T.create()

    abstract fun <T : AbstractTableSchema>T.drop()

    abstract fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Observable<Id<P, T>>

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

    class DocumentSchemaQueryObservableParams<T : DocumentSchema<P, C>, P, C>(val schema: T, val query: Op? = null,
                                      var skip: Int? = null, var take: Int? = null, var subscribed: Boolean = false)


    inner class DocumentSchemaQueryObservable<T : DocumentSchema<P, C>, P, C>(val params: DocumentSchemaQueryObservableParams<T, P, C>,
                                                          onSubscribe: OnSubscribeFunc<C>) : Observable<C>(onSubscribe) {
        override fun skip(num: Int): Observable<C> {
            if (params.subscribed) {
                return super<Observable>.skip(num)
            } else {
                params.skip = num
                return this
            }
        }
        override fun take(num: Int): Observable<C> {
            if (params.subscribed) {
                return super<Observable>.take(num)
            } else {
                params.take = num
                return this
            }
        }

        fun remove(): Observable<Int> {
            return Observable.create(OnSubscribeFunc<Int> { observer ->
                try {
                    observer.onNext(delete(params.schema, params.query!!))
                    observer.onCompleted()
                } catch (e: Throwable) {
                    observer.onError(e)
                }
                Subscriptions.empty()!!
            })
        }

        fun <X> projection(x: T.() -> X): X {
            val xx = params.schema.x()
            val projectionParams = TableSchemaProjectionQueryObservableParams<TableSchema<Any?>, Any?, Any?>(params.schema as TableSchema<Any?>,
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
            tableSchemaProjectionObservableThreadLocale.set(TableSchemaProjectionQueryObservable(projectionParams, OnSubscribeFunc<Any?> { observer ->
                onSubscribe2(projectionParams, observer)
            }))
            return params.schema.x()
        }
    }

    class TableSchemaProjectionQueryObservableParams<T : TableSchema<P>, P, V>(val table: T, val projection: List<AbstractColumn<*, *, *>>, val query: Op? = null,
                                                                var skip: Int? = null, var take: Int? = null, var subscribed: Boolean = false)

    class TableSchemaProjectionQueryObservable<T : TableSchema<P>, P, V>(val params: TableSchemaProjectionQueryObservableParams<T, P, V>,
                                                          onSubscribe: OnSubscribeFunc<V>) : Observable<V>(onSubscribe) {
        override fun skip(num: Int): Observable<V> {
            if (params.subscribed) {
                return super<Observable>.skip(num)
            } else {
                params.skip = num
                return this
            }
        }
        override fun take(num: Int): Observable<V> {
            if (params.subscribed) {
                return super<Observable>.take(num)
            } else {
                params.take = num
                return this
            }
        }
    }

    abstract fun <T : DocumentSchema<P, C>, P, C> onSubscribe(params: DocumentSchemaQueryObservableParams<T, P, C>,
                                                              observer: Observer<in C>): Subscription

    abstract fun <T : TableSchema<P>, P, V> onSubscribe2(params: TableSchemaProjectionQueryObservableParams<T, P, V>,
                                                              observer: Observer<in V>): Subscription

    fun <T: DocumentSchema<P, C>, P, C> T.find(query: T.() -> Op = { NoOp }): DocumentSchemaQueryObservable<T, P, C> {
        val params = DocumentSchemaQueryObservableParams<T, P, C>(this, query())
        return DocumentSchemaQueryObservable(params, OnSubscribeFunc<C> { observer ->
            onSubscribe(params, observer)
        })
    }

    abstract fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, *>): C
    abstract fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, *>): Int
    abstract fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, *>, v: C)
}
