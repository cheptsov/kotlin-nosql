package kotlin.nosql

class RangeQuery<T: TableSchema, C>(val query: Query1<T, List<C>>, val range: IntRange) {
}