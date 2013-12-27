package kotlin.nosql

import java.sql.Connection
import java.sql.Driver
import java.util.regex.Pattern
import java.sql.Statement
import java.sql.ResultSet

abstract class Session () {
    abstract fun <T: Table>T.create()

    abstract fun <T: Table>T.drop()

    /*
    fun <T: Table, A, B> T.insert(template: T.() -> Template2<T, A, B>): InsertQuery2<T, A, B> {
        val tt = template()
        return InsertQuery2(tt.a, tt.b)
    }
    fun <T: Table, A, B, C, D> T.insert(template: T.() -> Template4<T, A, B, C, D>): InsertQuery4<T, A, B, C, D> {
        val tt = template()
        return InsertQuery4(tt.a, tt.b, tt.c, tt.d)
    }*/

    abstract fun <T : Table> insert(columns: Array<Pair<Column<*, T>, *>>)

    abstract fun <T:Table> T.delete(op: T.() -> Op)

    abstract fun <T: Table> update(query: UpdateQuery<T>)

    abstract fun <T: Table, C> Column<C, T>.forEach(statement: (C) -> Unit)

    abstract fun <T : Table, C> Column<C, T>.iterator(): Iterator<C>

    abstract fun <T : Table, C, M> Column<C, T>.map(statement: (C) -> M): List<M>

    abstract fun <T: Table, C> Column<C, T>.find(op: T.() -> Op) : C?

    abstract fun <T: Table, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit)
    abstract fun <T: Table, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>>
    abstract fun <T : Table, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M>

    abstract fun <T: Table, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit)
    abstract fun <T: Table, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>>

    fun <T: Table, A, B> Template2<T, A, B>.filter(op: T.() -> Op): Query2<T, A, B> {
        return Query2<T, A, B>(a, b).where(table.op())
    }

    fun <A, B> values(a: A, b: B): Pair<A, B> {
        return Pair(a, b)
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
}
