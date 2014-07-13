package kotlinx.nosql

class ColumnDecuple<S : AbstractSchema, A, B, C, D, E, F, G, H, J, K>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                                   val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                                   val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>,
                                                                   val g: AbstractColumn<G, S, *>, val h: AbstractColumn<H, S, *>,
                                                                   val i: AbstractColumn<J, S, *>, val j: AbstractColumn<K, S, *>): ColumnQueryWrapper<Decuple<A, B, C, D, E, F, G, H, J, K>>() {
}