package kotlinx.nosql

class RangeQuery<T: AbstractTableSchema, A: AbstractColumn<List<C>, T, C>, C>(val query: Query1<T, A, List<C>>, val range: IntRange) {
}