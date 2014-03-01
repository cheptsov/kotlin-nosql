package kotlin.nosql

class RangeQuery<T: AbstractTableSchema, C>(val query: Query1<T, List<C>>, val range: IntRange) {
}