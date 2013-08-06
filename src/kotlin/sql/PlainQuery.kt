package kotlin.sql

import java.sql.Connection
import java.util.HashSet
import java.util.ArrayList
import java.sql.ResultSet

open class PlainQuery(val session: Session, val sql: String) {
    inner class Row {
        fun <C> get(column: Column<C, *>): C? {
            return rs!!.getObject(column.name) as C?
        }

        fun <C> get(name: String): C? {
            return rs!!.getObject(name) as C?
        }
    }

    var rs: ResultSet? = null
    val row = Row()

    fun <B> map(statement: (row: Row) -> B): List<B> {
        val results = ArrayList<B>()
        forEach {
            results.add(statement(it))
        }
        return results
    }

    fun forEach(statement: (row: Row) -> Unit) {
        rs = session.connection.createStatement()?.executeQuery(sql.toString())!!
        while (rs!!.next()) {
            statement(row)
        }
    }
}