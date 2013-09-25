package kotlin.sql

class FilterQuery<T: Table>(val table: T, val op: Op) {
}