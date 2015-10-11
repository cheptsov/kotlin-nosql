package kotlinx.nosql

class ColumnTriple<S : AbstractSchema, A, B, C>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>, val c: AbstractColumn<C, S, *>): ColumnQueryWrapper<Triple<A, B, C>>() {
    operator fun <D> plus(d: AbstractColumn<D, S, *>): ColumnQuadruple<S, A, B, C, D> {
        return ColumnQuadruple(a, b, c, d)
    }
}