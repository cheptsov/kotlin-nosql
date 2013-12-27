package kotlin.nosql

import java.sql.Connection
import java.sql.Statement

/*
class InsertQuery2<T: Table, A, B>(val p1: Column<A, T>, val p2: Column<B, T>) {
    fun values(a: A, b: B) {
        Session.get().insert(array(Pair(p1, a), Pair(p2, b)))
    }
}

class InsertQuery4<T: Table, A, B, C, D>(val p1: Column<A, T>, val p2: Column<B, T>, val p3: Column<C, T>, val p4: Column<D, T>) {
    fun values(a: A, b: B, c: C, d: D) {
        Session.get().insert(array(Pair(p1, a), Pair(p2, b), Pair(p3, c), Pair(p4, d)))
    }
}
*/