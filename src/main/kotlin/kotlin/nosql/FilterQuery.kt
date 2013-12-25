package kotlin.nosql

class FilterQuery<T: Table>(val table: T, val op: Op) {
}