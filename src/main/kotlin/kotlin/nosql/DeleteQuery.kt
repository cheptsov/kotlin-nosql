package kotlin.nosql

import java.sql.Connection

class DeleteQuery(val session: Session, val table: Table) {
    fun where(op: Op?) {
        val sql = StringBuilder("DELETE FROM ${session.identity(table)}")
        if (op != null) {
            sql.append(" WHERE ${op.toSQL()}")
        }
        println("SQL: " + sql)
        session.connection.createStatement()!!.executeUpdate(sql.toString())
    }
}