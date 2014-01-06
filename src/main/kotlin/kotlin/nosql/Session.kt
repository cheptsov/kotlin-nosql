package kotlin.nosql

import java.util.ArrayList

abstract class Session () {
    abstract fun <T : TableSchema>T.create()

    abstract fun <T : TableSchema>T.drop()

    /*
    fun <T: Table, A, B> T.insert(template: T.() -> Template2<T, A, B>): InsertQuery2<T, A, B> {
        val tt = template()
        return InsertQuery2(tt.a, tt.b)
    }
    fun <T: Table, A, B, C, D> T.insert(template: T.() -> Template4<T, A, B, C, D>): InsertQuery4<T, A, B, C, D> {
        val tt = template()
        return InsertQuery4(tt.a, tt.b, tt.c, tt.d)
    }*/

    abstract fun <T : AbstractSchema> insert(columns: Array<Pair<Column<*, T>, *>>)

    /*abstract fun <T : Table> update(columns: Array<Pair<Column<*, T>, *>>)*/

    abstract fun <T : AbstractSchema> delete(table: T, op: Op)

    /*abstract fun <T: Table> update(query: UpdateQuery<T>)*/

    abstract fun <T : TableSchema, C> Query1<T, C>.set(c: () -> C)

    abstract fun <T : TableSchema, C> Column<C, T>.forEach(statement: (C) -> Unit)

    abstract fun <T : TableSchema, C> Column<C, T>.iterator(): Iterator<C>

    abstract fun <T : TableSchema, C, M> Column<C, T>.map(statement: (C) -> M): List<M>

    abstract fun <T : TableSchema, C> Column<C, T>.get(op: T.() -> Op): C?

    abstract fun <T : TableSchema, A, B> Template2<T, A, B>.get(op: T.() -> Op): Pair<A, B>?

    abstract fun <T : TableSchema, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit)
    abstract fun <T : TableSchema, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>>
    abstract fun <T : TableSchema, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M>

    abstract fun <T : TableSchema, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit)
    abstract fun <T : TableSchema, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>>

    fun <T : TableSchema, A, B> Template2<T, A, B>.filter(op: T.() -> Op): Query2<T, A, B> {
        return Query2<T, A, B>(a, b).where(table.op())
    }

    fun <T : TableSchema, A> Column<A, T>.filter(op: T.() -> Op): Query1<T, A> {
        return Query1<T, A>(this).where(table.op())
    }

    fun <T : TableSchema, A> Query1<T, List<A>>.range1(range: () -> IntRange): RangeQuery<T, A> {
        return RangeQuery<T, A>(this, range())
    }

    fun <T : TableSchema, A> Query1<T, List<A>>.get(range: () -> IntRange): List<A> {
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

    fun <T : TableSchema, A> Query1<T, List<A>>.range(range: () -> IntRange): RangeQuery<T, A> {
        return RangeQuery<T, A>(this, range())
    }

    abstract fun <T : TableSchema, C> RangeQuery<T, C>.forEach(st: (c: C) -> Unit)

    fun <T : TableSchema, C> RangeQuery<T, C>.list(): List<C> {
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

    fun <T: DocumentSchema<C>, C> T.iterator(): Iterator<C> {
        val results = ArrayList<C>()
        return results.iterator()
    }

    fun <T: DocumentSchema<C>, C> T.filter(op: T.() -> Op): Iterator<C> {
        val results = ArrayList<C>()
        return results.iterator()
    }

    abstract fun <T: DocumentSchema<C>, C> T.get(op: T.() -> Op): C


    abstract fun <T : TableSchema, C, CC : Collection<*>> Query1<T, CC>.add(c: () -> C)
    abstract fun <T : TableSchema> Query1<T, Int>.add(c: () -> Int): Int

    abstract fun <T : KeyValueSchema, C> T.get(c: T.() -> Column<C, T>): C
    abstract fun <T : KeyValueSchema> T.next(c: T.() -> Column<Int, T>): Int
    abstract fun <T : KeyValueSchema, C> T.set(c: () -> Column<C, T>, v: C)
    abstract fun <T : TableSchema> Column<Int, T>.add(c: () -> Int): Int
    abstract fun <T : TableSchema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit)
}
