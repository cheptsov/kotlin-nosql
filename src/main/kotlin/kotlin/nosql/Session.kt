package kotlin.nosql

import java.util.ArrayList

abstract class Session () {
    abstract fun <T : Schema>T.create()

    abstract fun <T : Schema>T.drop()

    /*
    fun <T: Table, A, B> T.insert(template: T.() -> Template2<T, A, B>): InsertQuery2<T, A, B> {
        val tt = template()
        return InsertQuery2(tt.a, tt.b)
    }
    fun <T: Table, A, B, C, D> T.insert(template: T.() -> Template4<T, A, B, C, D>): InsertQuery4<T, A, B, C, D> {
        val tt = template()
        return InsertQuery4(tt.a, tt.b, tt.c, tt.d)
    }*/

    abstract fun <T : Schema> insert(columns: Array<Pair<Column<*, T>, *>>)

    /*abstract fun <T : Table> update(columns: Array<Pair<Column<*, T>, *>>)*/

    abstract fun <T : Schema> delete(table: T, op: Op)

    /*abstract fun <T: Table> update(query: UpdateQuery<T>)*/

    abstract fun <T : Schema, C> Query1<T, C>.set(c: () -> C)

    abstract fun <T : Schema, C> Column<C, T>.forEach(statement: (C) -> Unit)

    abstract fun <T : Schema, C> Column<C, T>.iterator(): Iterator<C>

    abstract fun <T : Schema, C, M> Column<C, T>.map(statement: (C) -> M): List<M>

    abstract fun <T : Schema, C> Column<C, T>.get(op: T.() -> Op): C?

    abstract fun <T : Schema, A, B> Template2<T, A, B>.get(op: T.() -> Op): Pair<A, B>?

    abstract fun <T : Schema, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit)
    abstract fun <T : Schema, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>>
    abstract fun <T : Schema, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M>

    abstract fun <T : Schema, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit)
    abstract fun <T : Schema, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>>

    fun <T : Schema, A, B> Template2<T, A, B>.filter(op: T.() -> Op): Query2<T, A, B> {
        return Query2<T, A, B>(a, b).where(table.op())
    }

    fun <T : Schema, A> Column<A, T>.filter(op: T.() -> Op): Query1<T, A> {
        return Query1<T, A>(this).where(table.op())
    }

    fun <T : Schema, A> Query1<T, List<A>>.range1(range: () -> IntRange): RangeQuery<T, A> {
        return RangeQuery<T, A>(this, range())
    }

    fun <T : Schema, A> Query1<T, List<A>>.get(range: () -> IntRange): List<A> {
        return RangeQuery<T, A>(this, range()).list()
    }

    /*fun <T: Schema, A> RangeQuery<T, A>.get(st: (A) -> Unit) {
        val list = list()
        if (list != null) {
            for (a in list) {
                st(a)
            }
        }
    }*/

    fun <T : Schema, A> Query1<T, List<A>>.range(range: () -> IntRange): RangeQuery<T, A> {
        return RangeQuery<T, A>(this, range())
    }

    abstract fun <T : Schema, C> RangeQuery<T, C>.forEach(st: (c: C) -> Unit)

    fun <T : Schema, C> RangeQuery<T, C>.list(): List<C> {
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

        fun get(): Session {
            return threadLocale.get()!!
        }
    }
    abstract fun <T : Schema, C, CC : Collection<*>> Query1<T, CC>.add(c: () -> C)
    abstract fun <T : Schema> Query1<T, Int>.add(c: () -> Int): Int

    abstract fun <T : Schema, C> T.get(c: T.() -> Column<C, T>): C
    abstract fun <T : Schema> T.next(c: T.() -> Column<Int, T>): Int
    abstract fun <T : Schema, C> T.set(c: () -> Column<C, T>, v: C)
    abstract fun <T : Schema> Column<Int, T>.add(c: () -> Int): Int
    abstract fun <T : Schema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit)
}
