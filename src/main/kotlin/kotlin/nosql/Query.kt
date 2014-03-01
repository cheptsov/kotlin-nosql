package kotlin.nosql

import java.sql.Connection
import java.util.HashSet
import java.util.ArrayList

abstract class Query<C, T: AbstractTableSchema>(val fields: Array<Field<*, T>>) {
    var op: Op? = null;

    open fun where(op: Op): Query<C, T> {
        this.op = op
        return this
    }

    /*
    fun or(op: Op): Query<C, T> {
        this.op = OrOp(this.op!!, op)
        return this
    }

    fun and(op: Op): Query<C, T> {
        this.op = AndOp(this.op!!, op)
        return this
    }*/

    /*fun groupBy(vararg columns: Column<*, *>): Query<T> {
        for (column in columns) {
            groupedByColumns.add(column)
        }
        return this
    }*/

    /*fun <B> map(statement: (row: T) -> B): List<B> {
        val results = ArrayList<B>()
        forEach {
            results.add(statement(it))
        }
        return results
    }

    fun forEach(statement: (row: T) -> Unit) {
        session.forEach(this, statement)
    }*/
}

fun <T: AbstractTableSchema, C> Query<Set<C>, T>.remove(value: () -> C) {
    throw UnsupportedOperationException()
}

class Query2<T: AbstractTableSchema, A, B>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>): Query<Pair<A, B>, T>(array(a, b)) {
    override fun where(op: Op): Query2<T, A, B> {
        return super.where(op) as Query2<T, A, B>
    }
}

class Query1<T: AbstractTableSchema, A>(val a: AbstractColumn<A, T, *>): Query<A, T>(array(a)) {
    override fun where(op: Op): Query1<T, A> {
        return super.where(op) as Query1<T, A>
    }
}
