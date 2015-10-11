package kotlinx.nosql

class ColumnQuadruple<S : AbstractSchema, A, B, C, D>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>, val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>): ColumnQueryWrapper<Quadruple<A, B, C, D>>() {
    operator fun <E> plus(e: AbstractColumn<E, S, *>): ColumnQuintuple<S, A, B, C, D, E> {
        return ColumnQuintuple(a, b, c, d, e)
    }

    fun insert(statement: () -> Quadruple<A, B, C, D>) {
        val tt = statement()
        Session.current<Session>().insert(arrayOf(Pair(a, tt.component1()), Pair(b, tt.component2()), Pair(c, tt.component3()), Pair(d, tt.component4())))
    }
}