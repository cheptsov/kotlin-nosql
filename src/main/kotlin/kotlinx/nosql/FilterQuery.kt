package kotlinx.nosql

class FilterQuery<T: AbstractTableSchema>(val table: T, val op: Op) {
}

/*
fun <T: Table> FilterQuery<T>.set(body: T.(UpdateQuery<T>) -> Unit): UpdateQuery<T> {
    val answer = UpdateQuery(table, op)
    table.body(answer)
    Session.get().update(answer)
    return answer
}
*/

