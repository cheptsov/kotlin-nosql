package kotlinx.nosql

import java.util.ArrayList

abstract class Session () {
    abstract fun <T : AbstractTableSchema>T.create()

    abstract fun <T : AbstractTableSchema>T.drop()

    abstract fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T>

    abstract fun <T : Schema> insert(columns: Array<Pair<AbstractColumn<*, T, *>, *>>)

    abstract fun <T : Schema> delete(table: T, op: Op)

    abstract fun <T : AbstractTableSchema, A: AbstractColumn<C, T, *>, C> Query1<T, A, C>.set(c: C)
    abstract fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.set(av: A, bv: B)
    abstract fun <T : AbstractTableSchema, A, B, C> Query3<T, A, B, C>.set(av: A, bv: B, cv: C)
    abstract fun <T : AbstractTableSchema, A, B, C, D> Query4<T, A, B, C, D>.set(av: A, bv: B, cv: C, dv: D)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E> Query5<T, A, B, C, D, E>.set(av: A, bv: B, cv: C, dv: D, ev: E)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F> Query6<T, A, B, C, D, E, F>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F, G> Query7<T, A, B, C, D, E, F, G>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H> Query8<T, A, B, C, D, E, F, G, H>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, I> Query9<T, A, B, C, D, E, F, G, H, I>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H, iv: I)
    abstract fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, I, J> Query10<T, A, B, C, D, E, F, G, H, I, J>.set(av: A, bv: B, cv: C, dv: D, ev: E, fv: F, gv: G, hv: H, iv: I, jv: J)

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

    fun <T : AbstractTableSchema, A, B, C, D, E, F, G, H, I, J> Template10<T, A, B, C, D, E, F, G, H, I, J>.filter(op: T.() -> Op): Query10<T, A, B, C, D, E, F, G, H, I, J> {
        return Query10<T, A, B, C, D, E, F, G, H, I, J>(a, b, c, d, e, f, g, h, i, j, Schema.current<T>().op())
    }

    fun <T : AbstractTableSchema, A: AbstractColumn<C, T, *>, C> A.filter(op: T.() -> Op): Query1<T, A, C> {
        return Query1<T, A, C>(this, Schema.current<T>().op())
    }

    fun <T : TableSchema<P>, A: AbstractColumn<C, T, *>, C, P> A.find(id: Id<P, T>): Query1<T, A, C> {
        return Query1<T, A, C>(this, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, P> Template2<T, A, B>.find(id: Id<P, T>): Query2<T, A, B> {
        return Query2<T, A, B>(a, b, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, P> Template3<T, A, B, C>.find(id: Id<P, T>): Query3<T, A, B, C> {
        return Query3<T, A, B, C>(a, b, c, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, P> Template4<T, A, B, C, D>.find(id: Id<P, T>): Query4<T, A, B, C, D> {
        return Query4<T, A, B, C, D>(a, b, c, d, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, P> Template5<T, A, B, C, D, E>.find(id: Id<P, T>): Query5<T, A, B, C, D, E> {
        return Query5<T, A, B, C, D, E>(a, b, c, d, e, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, P> Template6<T, A, B, C, D, E, F>.find(id: Id<P, T>): Query6<T, A, B, C, D, E, F> {
        return Query6<T, A, B, C, D, E, F>(a, b, c, d, e, f, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, G, P> Template7<T, A, B, C, D, E, F, G>.find(id: Id<P, T>): Query7<T, A, B, C, D, E, F, G> {
        return Query7<T, A, B, C, D, E, F, G>(a, b, c, d, e, f, g, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, G, H, P> Template8<T, A, B, C, D, E, F, G, H>.find(id: Id<P, T>): Query8<T, A, B, C, D, E, F, G, H> {
        return Query8<T, A, B, C, D, E, F, G, H>(a, b, c, d, e, f, g, h, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, G, H, J, P> Template9<T, A, B, C, D, E, F, G, H, J>.find(id: Id<P, T>): Query9<T, A, B, C, D, E, F, G, H, J> {
        return Query9<T, A, B, C, D, E, F, G, H, J>(a, b, c, d, e, f, g, h, j, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, A, B, C, D, E, F, G, H, J, K, P> Template10<T, A, B, C, D, E, F, G, H, J, K>.find(id: Id<P, T>): Query10<T, A, B, C, D, E, F, G, H, J, K> {
        return Query10<T, A, B, C, D, E, F, G, H, J, K>(a, b, c, d, e, f, g, h, i, j, Schema.current<T>().Id equal id)
    }

    fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.at(id: P): Query2<T, A, B> {
        return Query2<T, A, B>(a, b, Schema.current<T>().pk equal id)
    }

    fun <T : TableSchema<P>, P, A, B, C> Template3<T, A, B, C>.at(id: P): Query3<T, A, B, C> {
        return Query3<T, A, B, C>(a, b, c, Schema.current<T>().pk equal id)
    }

    fun <T : TableSchema<P>, P, A, B, C, D> Template4<T, A, B, C, D>.at(id: P): Query4<T, A, B, C, D> {
        return Query4<T, A, B, C, D>(a, b, c, d, Schema.current<T>().pk equal id)
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E> Template5<T, A, B, C, D, E>.at(id: P): Query5<T, A, B, C, D, E> {
        return Query5<T, A, B, C, D, E>(a, b, c, d, e, Schema.current<T>().pk equal id)
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F> Template6<T, A, B, C, D, E, F>.at(id: P): Query6<T, A, B, C, D, E, F> {
        return Query6<T, A, B, C, D, E, F>(a, b, c, d, e, f, Schema.current<T>().pk equal id)
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F, G> Template7<T, A, B, C, D, E, F, G>.at(id: P): Query7<T, A, B, C, D, E, F, G> {
        return Query7<T, A, B, C, D, E, F, G>(a, b, c, d, e, f, g, Schema.current<T>().pk equal id)
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H> Template8<T, A, B, C, D, E, F, G, H>.at(id: P): Query8<T, A, B, C, D, E, F, G, H> {
        return Query8<T, A, B, C, D, E, F, G, H>(a, b, c, d, e, f, g, h, Schema.current<T>().pk equal id)
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, J> Template9<T, A, B, C, D, E, F, G, H, J>.at(id: P): Query9<T, A, B, C, D, E, F, G, H, J> {
        return Query9<T, A, B, C, D, E, F, G, H, J>(a, b, c, d, e, f, g, h, j, Schema.current<T>().pk equal id)
    }

    fun <T : TableSchema<P>, P, A, B, C, D, E, F, G, H, J, K> Template10<T, A, B, C, D, E, F, G, H, J, K>.at(id: P): Query10<T, A, B, C, D, E, F, G, H, J, K> {
        return Query10<T, A, B, C, D, E, F, G, H, J, K>(a, b, c, d, e, f, g, h, i, j, Schema.current<T>().pk equal id)
    }

    //abstract fun <T : AbstractTableSchema, A: AbstractColumn<List<C>, T, C>, C> RangeQuery<T, A, C>.forEach(st: (c: C) -> Unit)

    /*fun <T : AbstractTableSchema, A: AbstractColumn<List<C>, T, C>, C> Query1<T, A, List<C>>.range(range: IntRange): RangeQuery<T, A, C> {
        return RangeQuery<T, A, C>(this, range)
    }*/

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

    fun <T: DocumentSchema<P, C>, C, P> T.get(id: Id<P, T>): C {
        return this.filter({ Id equal id }).next()
    }

    abstract fun <T: DocumentSchema<P, C>, P, C> T.filter(op: T.() -> Op): Iterator<C>


    abstract fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Collection<C>, C> Query1<T, A, CC>.add(c: C)
    abstract  fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Collection<C>, C> Query1<T, A, CC>.delete(c: A.() -> Op)
    abstract  fun <T : AbstractTableSchema, A: AbstractColumn<CC, T, out Any?>, CC: Set<C>, C> Query1<T, A, CC>.delete(c: C)
    // TODO TODO TODO Int?
    abstract fun <T : AbstractTableSchema, A: AbstractColumn<Int, T, Int>> Query1<T, A, Int>.add(c: Int): Int

    abstract fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, *>): C
    abstract fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, *>): Int
    abstract fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, *>, v: C)
    abstract fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit)
}
