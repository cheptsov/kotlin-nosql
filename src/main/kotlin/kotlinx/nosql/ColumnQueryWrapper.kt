package kotlinx.nosql

abstract class ColumnQueryWrapper<C> : Iterable<C> {
    override fun iterator(): Iterator<C> {
        val wrapper = TableSchemaProjectionQueryWrapper.get()
        return wrapper.iterator() as Iterator<C>
    }
}

fun <A, B> ColumnQueryWrapper<Pair<A, B>>.update(a: A, b: B): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b),
            wrapper.params.query!!)
}

fun <A, B, C> ColumnQueryWrapper<Triple<A, B, C>>.update(a: A, b: B, c: C): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b,
            wrapper.params.projection.get(2) to c),
            wrapper.params.query!!)
}

fun <A, B, C, D> ColumnQueryWrapper<Quadruple<A, B, C, D>>.update(a: A, b: B, c: C, d: D): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b,
            wrapper.params.projection.get(2) to c,
            wrapper.params.projection.get(3) to d),
            wrapper.params.query!!)
}

fun <A, B, C, D, E> ColumnQueryWrapper<Quintuple<A, B, C, D, E>>.update(a: A, b: B, c: C, d: D, e: E): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b,
            wrapper.params.projection.get(2) to c,
            wrapper.params.projection.get(3) to d,
            wrapper.params.projection.get(4) to e),
            wrapper.params.query!!)
}

fun <A, B, C, D, E, F> ColumnQueryWrapper<Sextuple<A, B, C, D, E, F>>.update(a: A, b: B, c: C, d: D, e: E, f: F): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b,
            wrapper.params.projection.get(2) to c,
            wrapper.params.projection.get(3) to d,
            wrapper.params.projection.get(4) to e,
            wrapper.params.projection.get(5) to f),
            wrapper.params.query!!)
}

fun <A, B, C, D, E, F, G> ColumnQueryWrapper<Septuple<A, B, C, D, E, F, G>>.update(a: A, b: B, c: C, d: D, e: E, f: F, g: G): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b,
            wrapper.params.projection.get(2) to c,
            wrapper.params.projection.get(3) to d,
            wrapper.params.projection.get(4) to e,
            wrapper.params.projection.get(5) to f,
            wrapper.params.projection.get(6) to g),
            wrapper.params.query!!)
}

fun <A, B, C, D, E, F, G, H> ColumnQueryWrapper<Octuple<A, B, C, D, E, F, G, H>>.update(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b,
            wrapper.params.projection.get(2) to c,
            wrapper.params.projection.get(3) to d,
            wrapper.params.projection.get(4) to e,
            wrapper.params.projection.get(5) to f,
            wrapper.params.projection.get(6) to g,
            wrapper.params.projection.get(7) to h),
            wrapper.params.query!!)
}

fun <A, B, C, D, E, F, G, H, J> ColumnQueryWrapper<Nonuple<A, B, C, D, E, F, G, H, J>>.update(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, j: J): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b,
            wrapper.params.projection.get(2) to c,
            wrapper.params.projection.get(3) to d,
            wrapper.params.projection.get(4) to e,
            wrapper.params.projection.get(5) to f,
            wrapper.params.projection.get(6) to g,
            wrapper.params.projection.get(7) to h,
            wrapper.params.projection.get(8) to j),
            wrapper.params.query!!)
}

fun <A, B, C, D, E, F, G, H, J, K> ColumnQueryWrapper<Decuple<A, B, C, D, E, F, G, H, J, K>>.update(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, j: J, k: K): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to a,
            wrapper.params.projection.get(1) to b,
            wrapper.params.projection.get(2) to c,
            wrapper.params.projection.get(3) to d,
            wrapper.params.projection.get(4) to e,
            wrapper.params.projection.get(5) to f,
            wrapper.params.projection.get(6) to g,
            wrapper.params.projection.get(7) to h,
            wrapper.params.projection.get(8) to j,
            wrapper.params.projection.get(9) to k),
            wrapper.params.query!!)
}