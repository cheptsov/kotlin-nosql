package kotlin.sql

import java.sql.Connection
import java.sql.Statement

class InsertQuery<T: Table>(val table: T, val statement: Statement) {
}

fun <T: Table> InsertQuery<T>.get(column: T.() -> Column<Int>): Int {
    val rs = statement.getGeneratedKeys()!!;
    if (rs.next()) {
        return rs.getInt(1)
    } else {
        throw IllegalStateException("No key generated after statement: $statement")
    }
}
