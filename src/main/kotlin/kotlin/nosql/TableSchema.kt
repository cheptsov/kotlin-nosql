package kotlin.nosql

import java.util.ArrayList
import java.sql.Statement
import java.util.HashMap

abstract class AbstractSchema(val name: String) {
    val columns = ArrayList<Column<*, *>>()
}

abstract class KeyValueSchema(name: String): AbstractSchema(name) {
}

abstract class TableSchema(name: String): AbstractSchema(name) {
    val primaryKeys = ArrayList<PKColumn<*, *>>()
}

abstract class DocumentSchema<V>(name: String, val valueClass: Class<V>) : TableSchema(name) {
}

fun <T: AbstractSchema, C> T.Column(name: String, valueClass: Class<C>): Column<C, T> {
    val column = Column<C, T>(this, name, when (valueClass.getName()) {
        "int" -> ColumnType.INTEGER
        "java.lang.String" -> ColumnType.STRING
        else -> throw UnsupportedOperationException(valueClass.getName())
    }, false)
    (columns as ArrayList<Column<*, T>>).add(column)
    return column
}

fun <T: TableSchema> T.delete(body: T.() -> Op) {
    FilterQuery(this, body()) delete { }
}


fun <T: TableSchema, X> T.columns(selector: T.() -> X): X {
    return selector();
}

fun <T: TableSchema, X> T.insert(selector: T.() -> X): X {
    return selector();
}


fun <T: TableSchema, B> FilterQuery<T>.map(statement: T.(Map<Any, Any>) -> B): List<B> {
    val results = ArrayList<B>()
    //Query
    return results
}

fun <T: TableSchema, A> T.template(a: Column<A, T>): Template1<T, A> {
    return Template1(this, a)
}

class Template1<T: TableSchema, A>(val table: T, val a: Column<A, T>) {
    fun invoke(av: A): Array<Pair<Column<*, T>, *>> {
        return array(Pair(a, av))
    }
}

class Quadruple<A1, A2, A3, A4>(val a1: A1, val a2: A2, val a3: A3, val a4: A4) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
}

class Quintuple<A1, A2, A3, A4, A5>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
    public fun component5(): A5 = a5
}

fun <T: TableSchema, A, B> T.template(a: Column<A, T>, b: Column<B, T>): Template2<T, A, B> {
    return Template2(this, a, b)
}

class Template2<T: AbstractSchema, A, B>(val table: T, val a: Column<A, T>, val b: Column<B, T>) {
    /*fun invoke(av: A, bv: B): Array<Pair<Column<*, T>, *>> {
        return array(Pair(a, av), Pair(b, bv))
    }*/

    fun <C> plus(c: Column<C, T>): Template3<T, A, B, C> {
        return Template3(table, a, b, c)
    }

    fun insert(statement: () -> Pair<A, B>) {
        val tt = statement()
        Session.get().insert(array(Pair(a, tt.first), Pair(b, tt.second)))
    }

    /*fun values(va: A, vb: B) {
        Session.get().insert(array(Pair(a, va), Pair(b, vb)))
    }*/
}

class Template3<T: AbstractSchema, A, B, C>(val table: T, val a: Column<A, T>, val b: Column<B, T>, val c: Column<C, T>) {
    fun invoke(av: A, bv: B, cv: C): Array<Pair<Column<*, T>, *>> {
        return array(Pair(a, av), Pair(b, bv), Pair(c, cv))
    }

    /*fun invoke(): List<Quad<A, B, C, D>> {
        val results = ArrayList<Quad<A, B, C, D>>()
        Query<Quad<A, B, C, D>>(Session.get(), array(a, b, c, d)).forEach{ results.add(it) }
        return results
    }*/

    fun values(va: A, vb: B, vc: C) {
        Session.get().insert(array(Pair(a, va), Pair(b, vb), Pair(c, vc)))
    }

    fun insert(statement: () -> Triple<A, B, C>) {
        val tt = statement()
        Session.get().insert(array(Pair(a, tt.component1()), Pair(b, tt.component2()), Pair(c, tt.component3())))
    }

    fun <D> plus(d: Column<D, T>): Template4<T, A, B, C, D> {
        return Template4(table, a, b, c, d)
    }
}

class Template4<T: AbstractSchema, A, B, C, D>(val table: T, val a: Column<A, T>, val b: Column<B, T>, val c: Column<C, T>, val d: Column<D, T>) {
    fun invoke(av: A, bv: B, cv: C, dv: D): Array<Pair<Column<*, T>, *>> {
        return array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv))
    }

    /*fun invoke(): List<Quad<A, B, C, D>> {
        val results = ArrayList<Quad<A, B, C, D>>()
        Query<Quad<A, B, C, D>>(Session.get(), array(a, b, c, d)).forEach{ results.add(it) }
        return results
    }*/

    fun values(va: A, vb: B, vc: C, vd: D) {
        Session.get().insert(array(Pair(a, va), Pair(b, vb), Pair(c, vc), Pair(d, vd)))
    }

    fun insert(statement: () -> Quadruple<A, B, C, D>) {
        val tt = statement()
        Session.get().insert(array(Pair(a, tt.component1()), Pair(b, tt.component2()), Pair(c, tt.component3()), Pair(d, tt.component4())))
    }
}