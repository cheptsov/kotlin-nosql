package kotlin.nosql

import java.util.ArrayList

abstract class Session () {
    abstract fun <T : AbstractTableSchema>T.create()

    abstract fun <T : AbstractTableSchema>T.drop()

    abstract fun <T : DocumentSchema<P, V>, P, V> T.insert(v: () -> V): P

    abstract fun <T : Schema> insert(columns: Array<Pair<AbstractColumn<*, T, *>, *>>)

    abstract fun <T : Schema> delete(table: T, op: Op)

    abstract fun <T : AbstractTableSchema, C> Query1<T, C>.set(c: () -> C)

    abstract fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.set(c: () -> Pair<A, B>)

    //abstract fun <T : AbstractTableSchema, C> AbstractColumn<C, T, *>.forEach(statement: (C) -> Unit)

    //abstract fun <T : AbstractTableSchema, C> AbstractColumn<C, T, *>.iterator(): Iterator<C>

    //abstract fun <T : AbstractTableSchema, C, M> AbstractColumn<C, T, *>.map(statement: (C) -> M): List<M>

    abstract fun <T : TableSchema<P>, P, C> AbstractColumn<C, T, *>.get(id: () -> P): C

    abstract fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.get(id: () -> P): Pair<A, B>
    abstract fun <T : TableSchema<P>, P, A, B, C> Template3<T, A, B, C>.get(id: () -> P): Triple<A, B, C>
    abstract fun <T : TableSchema<P>, P, A, B, C, D> Template4<T, A, B, C, D>.get(id: () -> P): Quadruple<A, B, C, D>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E> Template5<T, A, B, C, D, E>.get(id: () -> P): Quintuple<A, B, C, D, E>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F> Template6<T, A, B, C, D, E, F>.get(id: () -> P): Sextuple<A, B, C, D, E, F>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F, G> Template7<T, A, B, C, D, E, F, G>.get(id: () -> P): Septuple<A, B, C, D, E, F, G>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H> Template8<T, A, B, C, D, E, F, G, H>.get(id: () -> P): Octuple<A, B, C, D, E, F, G, H>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, J> Template9<T, A, B, C, D, E, F, G, H, J>.get(id: () -> P): Nonuple<A, B, C, D, E, F, G, H, J>
    abstract fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, J, K> Template10<T, A, B, C, D, E, F, G, H, J, K>.get(id: () -> P): Decuple<A, B, C, D, E, F, G, H, J, K>

    //abstract fun <T : AbstractTableSchema, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit)
    //abstract fun <T : AbstractTableSchema, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>>
    //abstract fun <T : AbstractTableSchema, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M>


    //abstract fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit)
    //abstract fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>>

    abstract fun <T : AbstractTableSchema, C> iterator(query: Query<C, T>): Iterator<C>

    fun <T : AbstractTableSchema, A, B> Template2<T, A, B>.filter(op: T.() -> Op): Query2<T, A, B> {
        return Query2<T, A, B>(a, b, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C> Template3<T, A, B, C>.filter(op: T.() -> Op): Query3<T, A, B, C> {
        return Query3<T, A, B, C>(a, b, c, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D> Template4<T, A, B, C, D>.filter(op: T.() -> Op): Query4<T, A, B, C, D> {
        return Query4<T, A, B, C, D>(a, b, c, d, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E> Template5<T, A, B, C, D, E>.filter(op: T.() -> Op): Query5<T, A, B, C, D, E> {
        return Query5<T, A, B, C, D, E>(a, b, c, d, e, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F> Template6<T, A, B, C, D, E, F>.filter(op: T.() -> Op): Query6<T, A, B, C, D, E, F> {
        return Query6<T, A, B, C, D, E, F>(a, b, c, d, e, f, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G> Template7<T, A, B, C, D, E, F, G>.filter(op: T.() -> Op): Query7<T, A, B, C, D, E, F, G> {
        return Query7<T, A, B, C, D, E, F, G>(a, b, c, d, e, f, g, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H> Template8<T, A, B, C, D, E, F, G, H>.filter(op: T.() -> Op): Query8<T, A, B, C, D, E, F, G, H> {
        return Query8<T, A, B, C, D, E, F, G, H>(a, b, c, d, e, f, g, h, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, J> Template9<T, A, B, C, D, E, F, G, H, J>.filter(op: T.() -> Op): Query9<T, A, B, C, D, E, F, G, H, J> {
        return Query9<T, A, B, C, D, E, F, G, H, J>(a, b, c, d, e, f, g, h, j, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, J, K> Template10<T, A, B, C, D, E, F, G, H, J, K>.filter(op: T.() -> Op): Query10<T, A, B, C, D, E, F, G, H, J, K> {
        return Query10<T, A, B, C, D, E, F, G, H, J, K>(a, b, c, d, e, f, g, h, j, k, Schema.current<T>().op())
    }

    fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.at(id: () -> P): Query2<T, A, B> {
        return Query2<T, A, B>(a, b, Schema.current<T>().pk equal id())
    }

    fun <T : TableSchema<P>, P, A, B, C> Template3<T, A, B, C>.at(id: () -> P): Query3<T, A, B, C> {
        return Query3<T, A, B, C>(a, b, c, Schema.current<T>().pk equal id())
    }

    fun <T : TableSchema<P>, P, A, B, C, D> Template4<T, A, B, C, D>.at(id: () -> P): Query4<T, A, B, C, D> {
        return Query4<T, A, B, C, D>(a, b, c, d, Schema.current<T>().pk equal id())
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E> Template5<T, A, B, C, D, E>.at(id: () -> P): Query5<T, A, B, C, D, E> {
        return Query5<T, A, B, C, D, E>(a, b, c, d, e, Schema.current<T>().pk equal id())
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F> Template6<T, A, B, C, D, E, F>.at(id: () -> P): Query6<T, A, B, C, D, E, F> {
        return Query6<T, A, B, C, D, E, F>(a, b, c, d, e, f, Schema.current<T>().pk equal id())
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F, G> Template7<T, A, B, C, D, E, F, G>.at(id: () -> P): Query7<T, A, B, C, D, E, F, G> {
        return Query7<T, A, B, C, D, E, F, G>(a, b, c, d, e, f, g, Schema.current<T>().pk equal id())
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H> Template8<T, A, B, C, D, E, F, G, H>.at(id: () -> P): Query8<T, A, B, C, D, E, F, G, H> {
        return Query8<T, A, B, C, D, E, F, G, H>(a, b, c, d, e, f, g, h, Schema.current<T>().pk equal id())
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, J> Template9<T, A, B, C, D, E, F, G, H, J>.at(id: () -> P): Query9<T, A, B, C, D, E, F, G, H, J> {
        return Query9<T, A, B, C, D, E, F, G, H, J>(a, b, c, d, e, f, g, h, j, Schema.current<T>().pk equal id())
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, J, K> Template10<T, A, B, C, D, E, F, G, H, J, K>.at(id: () -> P): Query10<T, A, B, C, D, E, F, G, H, J, K> {
        return Query10<T, A, B, C, D, E, F, G, H, J, K>(a, b, c, d, e, f, g, h, j, k, Schema.current<T>().pk equal id())
    }

    fun <T : AbstractTableSchema, A> AbstractColumn<A, T, *>.filter(op: T.() -> Op): Query1<T, A> {
        return Query1<T, A>(this, Schema.current<T>().op())
    }

    fun <T : TableSchema<P>, A, P> AbstractColumn<A, T, *>.at(id: () -> P): Query1<T, A> {
        return Query1<T, A>(this, Schema.current<T>().pk equal (id() as P))
    }

    fun <T : AbstractTableSchema, A> Query1<T, List<A>>.get(range: () -> IntRange): List<A> {
        return RangeQuery<T, A>(this, range()).list()
    }

    fun <T : AbstractTableSchema, A> Query1<T, List<A>>.range(range: () -> IntRange): RangeQuery<T, A> {
        return RangeQuery<T, A>(this, range())
    }

    abstract fun <T : AbstractTableSchema, C> RangeQuery<T, C>.forEach(st: (c: C) -> Unit)

    fun <T : AbstractTableSchema, C> RangeQuery<T, C>.list(): List<C> {
        val results = ArrayList<C>()
        forEach {
            results.add(it)
        }
        return results
    }

    fun <A, B> values(a: A, b: B): Pair<A, B> {
        return Pair(a, b)
    }

    fun <A, B, C> values(a: A, b: B, c: C): Triple<A, B, C> {
        return Triple(a, b, c)
    }

    fun <A, B, C, D> values(a: A, b: B, c: C, d: D): Quadruple<A, B, C, D> {
        return Quadruple(a, b, c, d)
    }

    class object {
        val threadLocale = ThreadLocal<Session>()

        fun current(): Session {
            return threadLocale.get()!!
        }
    }

    fun <T: DocumentSchema<P, C>, C, P> T.get(id: () -> P): C {
        return this.filter({ pk equal id() }).next()
    }

    abstract fun <T: DocumentSchema<P, C>, P, C> T.filter(op: T.() -> Op): Iterator<C>


    abstract fun <T : AbstractTableSchema, C, CC : Collection<*>> Query1<T, CC>.add(c: () -> C)
    abstract fun <T : AbstractTableSchema> Query1<T, Int>.add(c: () -> Int): Int

    abstract fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, *>): C
    abstract fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, *>): Int
    abstract fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, *>, v: C)
    abstract fun <T : AbstractTableSchema> AbstractColumn<Int, T, *>.add(c: () -> Int): Int
    abstract fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit)
}
