package kotlinx.nosql

class ColumnQuintuple<S : AbstractSchema, A, B, C, D, E>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                   val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                   val e: AbstractColumn<E, S, *>): ColumnQueryWrapper<Quintuple<A, B, C, D, E>>() {
    operator fun <F> plus(f: AbstractColumn<F, S, *>): ColumnSextuple<S, A, B, C, D, E, F> {
        return ColumnSextuple(a, b, c, d, e, f)
    }
}