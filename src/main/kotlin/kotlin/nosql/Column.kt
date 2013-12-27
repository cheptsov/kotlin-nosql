package kotlin.nosql

import java.util.ArrayList

open class Column<C, T: Table>(table: T, val name: String, val columnType: ColumnType, val _nullable: Boolean, val length: Int) : Field<C, T>(table) {
    fun eq(other: Expression): Op {
        return EqualsOp(this, other)
    }

    fun eq(other: C): Op {
        return EqualsOp(this, LiteralOp(other))
    }

    fun like(other: String): Op {
        return LikeOp(this, LiteralOp(other))
    }

    fun toString(): String {
        return "${table.tableName}.$name"
    }

    fun id(): PKColumn<C, T> {
        (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
        val column = PKColumn<C, T>(table, name, columnType, length)
        (table.tableColumns as ArrayList<Column<*, T>>).add(column)
        (table.primaryKeys as ArrayList<PKColumn<*, T>>).add(column)
        return column
    }

    fun invoke(av: C): Array<Pair<Column<*, T>, *>> {
        return array(Pair(this, av))
    }

    fun <C2> plus(c: Column<C2, T>): Template2<T, C, C2> {
        return Template2(table, this, c) as Template2<T, C, C2>
    }
}

fun <C, T : Table> Column<C, T>.nullable(): Column<C?, T> {
    (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
    val column = (Column<C?, T>(table, name, columnType, true, length)) as Column<C?, T>
    (table.tableColumns as ArrayList<Column<*, T>>).add(column)
    return column
}

val Column<*, *>.isNull: Op
    get() {
        return IsNullOp(this)
    }

fun Column<*, *>.eq(other: Expression): Op {
    return EqualsOp(this, other)
}

open class PKColumn<C, T: Table>(table: T, name: String, columnType: ColumnType, length: Int) : Column<C, T>(table, name, columnType, false, length) {
}

fun <T:Table> PKColumn<Int, T>.generated(): GeneratedPKColumn<Int, T> {
    (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
    (table.primaryKeys as ArrayList<PKColumn<*, T>>).remove(this)
    val column = GeneratedPKColumn<Int, T>(table, name, columnType, length)
    (table.tableColumns as ArrayList<Column<*, T>>).add(column)
    (table.primaryKeys as ArrayList<PKColumn<*, T>>).add(column)
    return column
}

fun <T:Table> Column<Int, T>.generated(): GeneratedColumn<Int, T> {
    (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
    (table.primaryKeys as ArrayList<PKColumn<*, T>>).remove(this)
    val column = GeneratedColumn<Int, T>(table, name, columnType, length)
    (table.tableColumns as ArrayList<Column<*, T>>).add(column)
    (table.primaryKeys as ArrayList<Column<*, T>>).add(column)
    return column
}


class GeneratedPKColumn<C, T: Table>(table: T, name: String, columnType: ColumnType, length: Int) : PKColumn<C, T>(table, name, columnType, length), GeneratedValue<C> {
}

class GeneratedColumn<C, T: Table>(table: T, name: String, columnType: ColumnType, length: Int) : Column<C, T>(table, name, columnType, false, length), GeneratedValue<C> {
}