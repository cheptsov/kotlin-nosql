package kotlin.nosql

import java.sql.Connection
import java.util.HashSet
import java.util.ArrayList

abstract class Query<C, T: Table>(val fields: Array<Field<*, T>>) {
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


class Query2<T: Table, A, B>(val a: Column<A, T>, val b: Column<B, T>): Query<Pair<A, B>, T>(array(a, b)) {
    override fun where(op: Op): Query2<T, A, B> {
        return super.where(op) as Query2<T, A, B>
    }
}
