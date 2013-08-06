package kotlin.sql

import java.util.ArrayList

open class Table(name: String = "") {
    val tableName = if (name.length() > 0) name else this.javaClass.getSimpleName()

    val tableColumns = ArrayList<Column<*, *>>()

    val primaryKeys = ArrayList<PKColumn<*, *>>()
    val foreignKeys = ArrayList<ForeignKey>()

    val ddl: String
        get() = ddl()

    fun toString(): String {
        return tableName
    }

    private fun ddl(): String {
        var ddl = StringBuilder("CREATE TABLE ${Session.get().identity(this)}")
        if (tableColumns.size > 0) {
            ddl.append(" (")
            var c = 0;
            for (column in tableColumns) {
                ddl.append(Session.get().identity(column)).append(" ")
                when (column.columnType) {
                    ColumnType.INTEGER -> ddl.append("INT")
                    ColumnType.STRING -> ddl.append("VARCHAR(${column.length})")
                    else -> throw IllegalStateException()
                }
                ddl.append(" ")
                if (column is PKColumn<*, *>) {
                    ddl.append("PRIMARY KEY ")
                }
                if (column is GeneratedValue<*>) {
                    ddl.append(Session.get().autoIncrement(column)).append(" ")
                }
                if (column._nullable) {
                    ddl.append("NULL")
                } else {
                    ddl.append("NOT NULL")
                }
                c++
                if (c < tableColumns.size) {
                    ddl.append(", ")
                }
            }
            ddl.append(")")
        }
        return ddl.toString()
    }

    fun create() {
        println("SQL: " + ddl.toString())
        Session.get().connection.createStatement()?.executeUpdate(ddl.toString())
        if (foreignKeys.size > 0) {
            for (foreignKey in foreignKeys) {
                val fKDdl = Session.get().foreignKey(foreignKey);
                println("SQL: " + fKDdl)
                Session.get().connection.createStatement()?.executeUpdate(fKDdl)
            }
        }
    }

    fun drop() {
        val ddl = StringBuilder("DROP TABLE ${Session.get().identity(this)}")
        println("SQL: " + ddl.toString())
        Session.get().connection.createStatement()?.executeUpdate(ddl.toString())
    }
}

fun <T: Table> T.integer(name: String): Column<Int, T> {
    return column(name, ColumnType.INTEGER)
}

fun <T: Table> T.varchar(name: String, length: Int): Column<String, T> {
    return column(name, ColumnType.STRING, length = length)
}

private fun <C, T: Table> T.column(name: String, columnType: ColumnType, length: Int = 0, autoIncrement: Boolean = false): Column<C, T> {
    val column = Column<C, T>(this, name, columnType, false, length)
    (tableColumns as ArrayList<Column<*, T>>).add(column)
    return column
}

/*
fun <T: Table, C> T.insert(column: T.() -> Pair<Column<C>, C>): InsertQuery {
    return Session.get().insert(array(column) as Array<Pair<Column<*>, *>>)
}
*/

fun <T:Table> T.filter(body: T.() -> Op): FilterQuery<T> {
    return FilterQuery(this, body())
}

fun <T: Table, A, B> Template2<T, A, B>.filter(op: T.() -> Op): Query<Pair<A, B>> {
    return Query<Pair<A, B>>(Session.get(), array(a, b)).where(table.op())
}

fun <T: Table> FilterQuery<T>.update(body: T.(UpdateQuery<T>) -> Unit): UpdateQuery<T> {
    val answer = UpdateQuery(table, op)
    table.body(answer)
    answer.execute(Session.get())
    return answer
}

fun <T:Table> T.delete(op: T.() -> Op) {
    DeleteQuery(Session.get(), this).where(op())
}

fun <T:Table> T.deleteAll() {
    DeleteQuery(Session.get(), this).where(null)
}

fun <T: Table, B> FilterQuery<T>.map(statement: T.(Map<Any, Any>) -> B): List<B> {
    val results = ArrayList<B>()
    //Query
    return results
}

fun <T: Table, A> T.template(a: Column<A, T>): Template1<T, A> {
    return Template1(this, a)
}

class Template1<T: Table, A>(val table: T, val a: Column<A, T>) {
    fun invoke(av: A): Array<Pair<Column<*, T>, *>> {
        return array(Pair(a, av))
    }

    /*fun <T2: Table, A2> multiply(t2: Template1<T2, A2>): Template1t1<T, A, T2, A2> {
        return Template1t1<T, A, T2, A2>(table, a, t2.table, t2.a)
    }*/
}

class Template1t1<T1: Table, A1, T2: Table, A2>(val table1: T1, val a1: Column<A1, T1>, val table2: T2, val a2: Column<A2, T2>) {
}

class Template3t1<T1: Table, A1, A2, A3, T2: Table, A4>(val table1: T1, val a1: Column<A1, T1>, val a2: Column<A2, T1>, val a3: Column<A3, T1>, val table2: T2, val a4: Column<A4, T2>) {
}

fun <T1: Table, A1, T2: Table, A2> Template1t1<T1, A1, T2, A2>.filter(op: () -> Op): Query<Pair<A1, A2>> {
    return Query<Pair<A1, A2>>(Session.get(), array(a1, a2)).where(op())
}

fun <T1: Table, A1, A2, A3, T2: Table, A4> Template3t1<T1, A1, A2, A3, T2, A4>.filter(op: () -> Op): Query<Quadruple<A1, A2, A3, A4>> {
    return Query<Quadruple<A1, A2, A3, A4>>(Session.get(), array(a1, a2, a3, a4)).where(op())
}

class Quadruple<A1, A2, A3, A4>(val a1: A1, val a2: A2, val a3: A3, val a4: A4) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
}

fun <T: Table, A, B> T.template(a: Column<A, T>, b: Column<B, T>): Template2<T, A, B> {
    return Template2(this, a, b)
}

class Template2<T: Table, A, B>(val table: T, val a: Column<A, T>, val b: Column<B, T>) {
    fun invoke(av: A, bv: B): Array<Pair<Column<*, T>, *>> {
        return array(Pair(a, av), Pair(b, bv))
    }

    fun invoke(): List<Pair<A, B>> {
        val results = ArrayList<Pair<A, B>>()
        Query<Pair<A, B>>(Session.get(), array(a, b)).forEach{ results.add(it) }
        return results
    }
}

fun <T: Table, A, B, C> T.template(a: Column<A, T>, b: Column<B, T>, c: Column<C, T>): Template3<T, A, B, C> {
    return Template3(this, a, b, c)
}

class Template3<T: Table, A, B, C>(val table: T, val a: Column<A, T>, val b: Column<B, T>, val c: Column<C, T>) {
    fun invoke(av: A, bv: B, cv: C): Array<Pair<Column<*, T>, *>> {
        return array(Pair(a, av), Pair(b, bv), Pair(c, cv))
    }

    fun invoke(): List<Triple<A, B, C>> {
        val results = ArrayList<Triple<A, B, C>>()
        Query<Triple<A, B, C>>(Session.get(), array(a, b, c)).forEach{ results.add(it) }
        return results
    }

    fun <T2: Table, D> times(t2: Column<D, T2>): Template3t1<T, A, B, C, T2, D> {
        return Template3t1(table, a, b, c, t2.table, t2)
    }
}

/*
class A<T> {
}

fun <T: Table> A<T>.forEach(statement: T.(Map<Any, Any>) -> Unit) {
}

fun <T: Table, B> A<T>.orderBy(statement: T.(Map<Any, Any>) -> B): A<T> {
    return this
}

fun <T: Table> T.filter(predicate: T.() -> Op) : A<T> {
    this.predicate();
    return A<T>();
}
*/

