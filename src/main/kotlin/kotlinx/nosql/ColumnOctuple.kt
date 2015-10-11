package kotlinx.nosql

class ColumnOctuple<S : AbstractSchema, A, B, C, D, E, F, G, H>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                            val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                            val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>,
                                                            val g: AbstractColumn<G, S, *>, val h: AbstractColumn<H, S, *>): ColumnQueryWrapper<Octuple<A, B, C, D, E, F, G, H>>() {
    operator fun <J> plus(j: AbstractColumn<J, S, *>): ColumnNonuple<S, A, B, C, D, E, F, G, H, J> {
        return ColumnNonuple(a, b, c, d, e, f, g, h, j)
    }
}