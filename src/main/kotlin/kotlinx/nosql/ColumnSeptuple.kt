package kotlinx.nosql

class ColumnSeptuple<S : AbstractSchema, A, B, C, D, E, F, G>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                         val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                         val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>,
                                                         val g: AbstractColumn<G, S, *>): ColumnQueryWrapper<Septuple<A, B, C, D, E, F, G>>() {
    fun <H> plus(h: AbstractColumn<H, S, *>): ColumnOctuple<S, A, B, C, D, E, F, G, H> {
        return ColumnOctuple(a, b, c, d, e, f, g, h)
    }
}