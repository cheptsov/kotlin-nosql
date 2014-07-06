package kotlinx.nosql

import java.util.ArrayList
import rx.Observable
import rx.Observable.OnSubscribeFunc
import com.mongodb.BasicDBObject
import rx.subscriptions.Subscriptions
import rx.Subscription
import rx.Observer
import kotlinx.nosql.Session.TableSchemaProjectionQueryObservable

class PaginatedStream<X>(val callback: (drop: Int?, take: Int?) -> Iterator<X>) : Stream<X> {
    var drop: Int? = null
    var take: Int? = null

    override fun iterator(): Iterator<X> {
        return callback(drop, take)
    }

    fun drop(n: Int) : PaginatedStream<X> {
        drop = n
        return this
    }
    fun take(n: Int) : PaginatedStream<X> {
        take = n
        return this
    }
}

val tableSchemaProjectionObservableThreadLocale = ThreadLocal<TableSchemaProjectionQueryObservable<out TableSchema<*>, *, *>>()

abstract class Session () {
    abstract fun <T : AbstractTableSchema>T.create()

    abstract fun <T : AbstractTableSchema>T.drop()

    abstract fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Observable<Id<P, T>>

    abstract fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<*, T, *>, *>>)

    abstract fun <T : AbstractSchema> delete(table: T, op: Op): Int

    /*
    abstract fun <T : AbstractTableSchema, A: AbstractColumn<C, T, *>, C> Query1<T, A, C>.set(c: C): Int
    abstract fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.set(av: A, bv: B): Int
    abstract fun <T : AbstractTableSchema, A, B, C> Query3<T, A, B, C>.set(av: A, bv: B, cv: C)
    abstract fun <T : AbstractTableSchema, A, B, C, D> Query4<T, A, B, C, D>.set(av: A, bv: B, cv: C, dv: D)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E> Query5<T, A, B, C, D, E>.set(av: A, bv: B, cv: C, dv: D, ev: E)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F> Query6<T, A, B, C, D, E, F>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F, G> Query7<T, A, B, C, D, E, F, G>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H> Query8<T, A, B, C, D, E, F, G, H>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, I> Query9<T, A, B, C, D, E, F, G, H, I>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H, iv: I)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, I, J> Query10<T, A, B, C, D, E, F, G, H, I, J>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H, iv: I, jv: J)
    */
    abstract fun <T : TableSchema<P>, P, C> AbstractColumn<C, T, *>.get(id: Id<P, T>): C

    abstract fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.get(id: Id<P, T>): Pair<A, B>
    abstract fun <T : TableSchema<P>, P, A, B, C> Template3<T, A, B, C>.get(id: Id<P, T>): Triple<A, B, C>
    abstract fun <T : TableSchema<P>, P, A, B, C, D> Template4<T, A, B, C, D>.get(id: Id<P, T>): Quadruple<A, B, C, D>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E> Template5<T, A, B, C, D, E>.get(id: Id<P, T>): Quintuple<A, B, C, D, E>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F> Template6<T, A, B, C, D, E, F>.get(id: Id<P, T>): Sextuple<A, B, C, D, E, F>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F, G> Template7<T, A, B, C, D, E, F, G>.get(id: Id<P, T>): Septuple<A, B, C, D, E, F, G>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H> Template8<T, A, B, C, D, E, F, G, H>.get(id: Id<P, T>): Octuple<A, B, C, D, E, F, G, H>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, J> Template9<T, A, B, C, D, E, F, G, H, J>.get(id: Id<P, T>): Nonuple<A, B, C, D, E, F, G, H, J>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, J, K> Template10<T, A, B, C, D, E, F, G, H, J, K>.get(id: Id<P, T>): Decuple<A, B, C, D, E, F, G, H, J, K>

    abstract fun <T : AbstractTableSchema, C> iterator(query: Query<C, T>): Iterator<C>

    // TODO TODO TODO Implement range
    // abstract fun <T: AbstractTableSchema, A: AbstractColumn<List<C>, T, C>, C>iterator(rangeQuery: RangeQuery<T, A, C>): Iterator<C>

    fun <T : AbstractTableSchema, A, B> Template2<T, A, B>.findAll(op: T.() -> Op): Query2<T, A, B> {
        return Query2<T, A, B>(a, b, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C> Template3<T, A, B, C>.findAll(op: T.() -> Op): Query3<T, A, B, C> {
        return Query3<T, A, B, C>(a, b, c, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D> Template4<T, A, B, C, D>.findAll(op: T.() -> Op): Query4<T, A, B, C, D> {
        return Query4<T, A, B, C, D>(a, b, c, d, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E> Template5<T, A, B, C, D, E>.findAll(op: T.() -> Op): Query5<T, A, B, C, D, E> {
        return Query5<T, A, B, C, D, E>(a, b, c, d, e, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F> Template6<T, A, B, C, D, E, F>.findAll(op: T.() -> Op): Query6<T, A, B, C, D, E, F> {
        return Query6<T, A, B, C, D, E, F>(a, b, c, d, e, f, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G> Template7<T, A, B, C, D, E, F, G>.findAll(op: T.() -> Op): Query7<T, A, B, C, D, E, F, G> {
        return Query7<T, A, B, C, D, E, F, G>(a, b, c, d, e, f, g, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H> Template8<T, A, B, C, D, E, F, G, H>.findAll(op: T.() -> Op): Query8<T, A, B, C, D, E, F, G, H> {
        return Query8<T, A, B, C, D, E, F, G, H>(a, b, c, d, e, f, g, h, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, J> Template9<T, A, B, C, D, E, F, G, H, J>.findAll(op: T.() -> Op): Query9<T, A, B, C, D, E, F, G, H, J> {
        return Query9<T, A, B, C, D, E, F, G, H, J>(a, b, c, d, e, f, g, h, j, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, I, J> Template10<T, A, B, C, D, E, F, G, H, I, J>.findAll(op: T.() -> Op): Query10<T, A, B, C, D, E, F, G, H, I, J> {
        return Query10<T, A, B, C, D, E, F, G, H, I, J>(a, b, c, d, e, f, g, h, i, j, AbstractSchema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A: AbstractColumn<C, T, *>, C> A.findAll(op: T.() -> Op): Query1<T, A, C> {
        return Query1<T, A, C>(this, AbstractSchema.current<T>().op())
    }

    fun <T : TableSchema<P>, A: AbstractColumn<C, T, *>, C, P> A.find(id: Id<P, T>): Query1<T, A, C> {
        return Query1<T, A, C>(this, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, P> Template2<T, A, B>.find(id: Id<P, T>): Query2<T, A, B> {
        return Query2<T, A, B>(a, b, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, P> Template3<T, A, B, C>.find(id: Id<P, T>): Query3<T, A, B, C> {
        return Query3<T, A, B, C>(a, b, c, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, P> Template4<T, A, B, C, D>.find(id: Id<P, T>): Query4<T, A, B, C, D> {
        return Query4<T, A, B, C, D>(a, b, c, d, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, P> Template5<T, A, B, C, D, E>.find(id: Id<P, T>): Query5<T, A, B, C, D, E> {
        return Query5<T, A, B, C, D, E>(a, b, c, d, e, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, P> Template6<T, A, B, C, D, E, F>.find(id: Id<P, T>): Query6<T, A, B, C, D, E, F> {
        return Query6<T, A, B, C, D, E, F>(a, b, c, d, e, f, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, G, P> Template7<T, A, B, C, D, E, F, G>.find(id: Id<P, T>): Query7<T, A, B, C, D, E, F, G> {
        return Query7<T, A, B, C, D, E, F, G>(a, b, c, d, e, f, g, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, G, H, P> Template8<T, A, B, C, D, E, F, G, H>.find(id: Id<P, T>): Query8<T, A, B, C, D, E, F, G, H> {
        return Query8<T, A, B, C, D, E, F, G, H>(a, b, c, d, e, f, g, h, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, G, H, J, P> Template9<T, A, B, C, D, E, F, G, H, J>.find(id: Id<P, T>): Query9<T, A, B, C, D, E, F, G, H, J> {
        return Query9<T, A, B, C, D, E, F, G, H, J>(a, b, c, d, e, f, g, h, j, AbstractSchema.current<T>().id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, G, H, J, K, P> Template10<T, A, B, C, D, E, F, G, H, J, K>.find(id: Id<P, T>): Query10<T, A, B, C, D, E, F, G, H, J, K> {
        return Query10<T, A, B, C, D, E, F, G, H, J, K>(a, b, c, d, e, f, g, h, i, j, AbstractSchema.current<T>().id equal id)
    }

    abstract fun update(schema: AbstractSchema, columnValues: Array<Pair<AbstractColumn<*, *, *>, *>>, op: Op): Int

    //abstract fun <T : AbstractTableSchema, A: AbstractColumn<List<C>, T, C>, C> RangeQuery<T, A, C>.forEach(st: (c: C) -> Unit)

    /*fun <T : AbstractTableSchema, A: AbstractColumn<List<C>, T, C>, C> Query1<T, A, List<C>>.range(range: IntRange): RangeQuery<T, A, C> {
        return RangeQuery<T, A, C>(this, range)
    }*/

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

    /*abstract fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Collection<C>, C> Query1<T, A, CC>.add(c: C)*/
    abstract  fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Collection<C>, C> Query1<T, A, CC>.delete(c: A.() -> Op)
    abstract  fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Set<C>, C> Query1<T, A, CC>.delete(c: C)
    // TODO TODO TODO Int?
    /*abstract fun <T : AbstractTableSchema, A: AbstractColumn<Int, T, Int>> Query1<T, A, Int>.add(c: Int): Int*/

    abstract fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, *>): C
    abstract fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, *>): Int
    abstract fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, *>, v: C)
    abstract fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit)
}
