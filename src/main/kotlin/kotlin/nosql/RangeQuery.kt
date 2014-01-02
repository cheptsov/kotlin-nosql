package kotlin.nosql

class RangeQuery<T: Schema, C>(val query: Query1<T, List<C>>, val range: IntRange) {
}