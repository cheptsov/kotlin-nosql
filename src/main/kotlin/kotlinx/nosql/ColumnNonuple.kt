package kotlinx.nosql

class ColumnNonuple<S : AbstractSchema, A, B, C, D, E, F, G, H, J>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                               val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                               val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>,
                                                               val g: AbstractColumn<G, S, *>, val h: AbstractColumn<H, S, *>,
                                                               val j: AbstractColumn<J, S, *>): ColumnQueryWrapper<Nonuple<A, B, C, D, E, F, G, H, J>>() {
    operator fun <K> plus(k: AbstractColumn<K, S, *>): ColumnDecuple<S, A, B, C, D, E, F, G, H, J, K> {
        return ColumnDecuple(a, b, c, d, e, f, g, h, j, k)
    }
}