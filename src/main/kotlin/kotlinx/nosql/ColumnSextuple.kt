package kotlinx.nosql

class ColumnSextuple<S : AbstractSchema, A, B, C, D, E, F>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                      val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                      val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>): ColumnQueryWrapper<Sextuple<A, B, C, D, E, F>>() {
    fun <G> plus(g: AbstractColumn<G, S, *>): ColumnSeptuple<S, A, B, C, D, E, F, G> {
        return ColumnSeptuple(a, b, c, d, e, f, g)
    }
}