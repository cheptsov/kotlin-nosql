package kotlinx.nosql

import java.util.ArrayList

abstract class Query<C, T : AbstractTableSchema>(val fields: Array<AbstractColumn<*, T, *>>, val op: Op) : Iterable<C> {
    override fun iterator(): Iterator<C> {
        return Session.current().iterator(this)
    }
}

// TODO TODO TODO: * - out Any?
class Query1<T : AbstractTableSchema, A: AbstractColumn<C, T, out Any?>, C>(val a: A, op: Op) : Query<C, T>(array(a as AbstractColumn<*, T, *>), op) {
}

class Query2<T : AbstractTableSchema, A, B>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>, op: Op) : Query<Pair<A, B>, T>(array(a, b), op) {
}

class Query3<T : AbstractTableSchema, A, B, C>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>,
                                               val c: AbstractColumn<C, T, *>, op: Op) : Query<Triple<A, B, C>, T>(array(a, b, c), op) {
}

class Query4<T : AbstractTableSchema, A, B, C, D>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>,
                                                  val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>, op: Op) : Query<Quadruple<A, B, C, D>, T>(array(a, b, c, d), op) {
}

class Query5<T : AbstractTableSchema, A, B, C, D, E>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>,
                                                  val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>,
                                                  val e: AbstractColumn<E, T, *>, op: Op) : Query<Quintuple<A, B, C, D, E>, T>(array(a, b, c, d, e), op) {
}

class Query6<T : AbstractTableSchema, A, B, C, D, E, F>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>,
                                                  val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>,
                                                  val e: AbstractColumn<E, T, *>, val f: AbstractColumn<F, T, *>, op: Op) : Query<Sextuple<A, B, C, D, E, F>, T>(array(a, b, c, d, e, f), op) {
}

class Query7<T : AbstractTableSchema, A, B, C, D, E, F, G>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>,
                                                  val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>,
                                                  val e: AbstractColumn<E, T, *>, val f: AbstractColumn<F, T, *>,
                                                  val g: AbstractColumn<G, T, *>, op: Op) : Query<Septuple<A, B, C, D, E, F, G>, T>(array(a, b, c, d, e, f, g), op) {
}

class Query8<T : AbstractTableSchema, A, B, C, D, E, F, G, H>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>,
                                                  val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>,
                                                  val e: AbstractColumn<E, T, *>, val f: AbstractColumn<F, T, *>,
                                                  val g: AbstractColumn<G, T, *>, val h: AbstractColumn<H, T, *>, op: Op) : Query<Octuple<A, B, C, D, E, F, G, H>, T>(array(a, b, c, d, e, f, g, h), op) {
}

class Query9<T : AbstractTableSchema, A, B, C, D, E, F, G, H, I>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>,
                                                  val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>,
                                                  val e: AbstractColumn<E, T, *>, val f: AbstractColumn<F, T, *>,
                                                  val g: AbstractColumn<G, T, *>, val h: AbstractColumn<H, T, *>,
                                                  val i: AbstractColumn<I, T, *>, op: Op) : Query<Nonuple<A, B, C, D, E, F, G, H, I>, T>(array(a, b, c, d, e, f, g, h, i), op) {
}

class Query10<T : AbstractTableSchema, A, B, C, D, E, F, G, H, I, J>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>,
                                                  val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>,
                                                  val e: AbstractColumn<E, T, *>, val f: AbstractColumn<F, T, *>,
                                                  val g: AbstractColumn<G, T, *>, val h: AbstractColumn<H, T, *>,
                                                  val i: AbstractColumn<I, T, *>, val j: AbstractColumn<J, T, *>, op: Op) : Query<Decuple<A, B, C, D, E, F, G, H, I, J>, T>(array(a, b, c, d, e, f, g, h, i, j), op) {
}