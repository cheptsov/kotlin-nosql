package kotlin.nosql

import java.sql.Connection
import java.util.LinkedHashMap

class UpdateQuery<T: Schema>(val table: T, val where: Op) {
    val values = LinkedHashMap<Column<*, T>, Any>()

    fun <C> set(column: Column<C, T>, value: C) {
        if (values containsKey column) {
            throw RuntimeException("$column is already initialized")
        }
        values[column] = value
    }
}