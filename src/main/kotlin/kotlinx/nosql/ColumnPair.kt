package kotlinx.nosql

class ColumnPair<S : AbstractSchema, A, B>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>): ColumnQueryWrapper<Pair<A, B>>() {
    fun <C> plus(c: AbstractColumn<C, S, *>): ColumnTriple<S, A, B, C> {
        return ColumnTriple(a, b, c)
    }
}

fun <S : KeyValueSchema, A, B> ColumnPair<S, A, B>.get(): Pair<A, B> {
    throw UnsupportedOperationException()
}