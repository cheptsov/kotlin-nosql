package kotlin.nosql

import java.util.ArrayList
import java.sql.Statement

open class Schema(name: String = "") {
    val tableName = if (name.length() > 0) name else this.javaClass.getSimpleName()

    val tableColumns = ArrayList<Column<*, *>>()

    val primaryKeys = ArrayList<PKColumn<*, *>>()

    fun toString(): String {
        return tableName
    }
}

fun <T: Schema> T.integer(name: String): Column<Int, T> {
    return column(name, ColumnType.INTEGER)
}

fun <T: Schema> T.integerPK(name: String): PKColumn<Int, T> {
    return column<Int, T>(name, ColumnType.INTEGER).primaryKey()
}

fun <T: Schema> T.nullableInteger(name: String): Column<Int?, T> {
    return column<Int, T>(name, ColumnType.INTEGER).nullable()
}

fun <T: Schema> T.nullableString(name: String): Column<String?, T> {
    return column<String, T>(name, ColumnType.STRING).nullable()
}

fun <T: Schema> T.setOfInteger(name: String): Column<Set<Int>, T> {
    return column(name, ColumnType.INTEGER_SET)
}

fun <T: Schema> T.setOfString(name: String): Column<Set<String>, T> {
    return column(name, ColumnType.STRING_SET)
}

fun <T: Schema> T.listOfInteger(name: String): Column<List<Int>, T> {
    return column(name, ColumnType.INTEGER_LIST)
}

fun <T: Schema> T.listOfString(name: String): Column<List<String>, T> {
    return column(name, ColumnType.STRING_LIST)
}

fun <T: Schema> T.string(name: String): Column<String, T> {
    return column(name, ColumnType.STRING)
}

fun <T: Schema> T.stringPK(name: String): PKColumn<String, T> {
    return column<String, T>(name, ColumnType.STRING).primaryKey()
}

private fun <C, T: Schema> T.column(name: String, attributeType: ColumnType): Column<C, T> {
    val column = Column<C, T>(this, name, attributeType, false)
    (tableColumns as ArrayList<Column<*, T>>).add(column)
    return column
}

fun <T: Schema> T.delete(body: T.() -> Op) {
    FilterQuery(this, body()) delete { }
}

/*fun <T: Table, A, B> Template2<T, A, B>.forEach(statement: (a: A, b: B) -> Unit) {
    Query<Pair<A, B>>(Session.get(), array(a, b)).forEach{
        val (a, b) = it
        statement(a, b);
    }
}*/

/*
fun <T: Table, A, B> Template2<T, A, B>.find(op: T.() -> Op): Pair<A, B> {
    var t: Pair<A, B>? = null
    filter(op).forEach { a,b ->
        t = Pair(a, b)
    }
    return t as Pair<A, B>;
}

fun <T: Table, C> Column<C, T>.filter(op: T.() -> Op): Query<C> {
    return Query<C>(Session.get(), array(this)).where(table.op())
}

fun <T: Table, C> Column<C, T>.find(op: T.() -> Op): C {
    var c: C? = null
    filter(op).forEach {
        c = it
    }
    return c as C;
}
*/

fun <T: Schema, X> T.columns(selector: T.() -> X): X {
    return selector();
}

fun <T: Schema, X> T.insert(selector: T.() -> X): X {
    return selector();
}


fun <T: Schema, B> FilterQuery<T>.map(statement: T.(Map<Any, Any>) -> B): List<B> {
    val results = ArrayList<B>()
    //Query
    return results
}

fun <T: Schema, A> T.template(a: Column<A, T>): Template1<T, A> {
    return Template1(this, a)
}

class Template1<T: Schema, A>(val table: T, val a: Column<A, T>) {
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

fun <T: Schema, A, B> T.template(a: Column<A, T>, b: Column<B, T>): Template2<T, A, B> {
    return Template2(this, a, b)
}

class Template2<T: Schema, A, B>(val table: T, val a: Column<A, T>, val b: Column<B, T>) {
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

class Template3<T: Schema, A, B, C>(val table: T, val a: Column<A, T>, val b: Column<B, T>, val c: Column<C, T>) {
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

class Template4<T: Schema, A, B, C, D>(val table: T, val a: Column<A, T>, val b: Column<B, T>, val c: Column<C, T>, val d: Column<D, T>) {
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