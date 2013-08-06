package kotlin.sql

import java.util.ArrayList

open class Column<C, T: Table>(val table: T, val name: String, val columnType: ColumnType, val _nullable: Boolean, val length: Int) : Field<C>() {
    fun equals(other: Expression): Op {
        return EqualsOp(this, other)
    }

    fun equals(other: C): Op {
        return EqualsOp(this, LiteralOp(other))
    }

    fun like(other: String): Op {
        return LikeOp(this, LiteralOp(other))
    }

    override fun toSQL(): String {
        return Session.get().fullIdentity(this);
    }

    fun toString(): String {
        return "${table.tableName}.$name"
    }

    fun <T2: Table, C2> times(c2: Column<C2, T2>): Template1t1<T, C, T2, C2> {
        return Template1t1<T, C, T2, C2>(table, this, c2.table, c2)
    }

    fun foreignKey(reference: Column<C, *>) : FKColumn<C, T> {
        (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
        val column = FKColumn<C, T>(table, name, columnType, length, reference)
        (table.tableColumns as ArrayList<Column<*, T>>).add(column)
        return column
    }

    fun primaryKey(): PKColumn<C, T> {
        (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
        val column = PKColumn<C, T>(table, name, columnType, length)
        (table.tableColumns as ArrayList<Column<*, T>>).add(column)
        (table.primaryKeys as ArrayList<PKColumn<*, T>>).add(column)
        return column
    }
}

fun <C, T : Table> Column<C, T>.nullable(): Column<C?, T> {
    (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
    val column = (Column<C?, T>(table, name, columnType, true, length)) as Column<C?, T>
    (table.tableColumns as ArrayList<Column<*, T>>).add(column)
    return column
}

fun <C, T : Table> FKColumn<C, T>.nullable(): FKColumn<C?, T> {
    (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
    val column = FKColumn<C, T>(table, name, columnType, length, reference) as FKColumn<C?, T>
    (table.tableColumns as ArrayList<Column<*, T>>).add(column)
    return column
}

val Column<*, *>.isNull: Op
    get() {
        return IsNullOp(this)
    }

fun Column<*, *>.equals(other: Expression): Op {
    return EqualsOp(this, other)
}

open class PKColumn<C, T: Table>(table: T, name: String, columnType: ColumnType, length: Int) : Column<C, T>(table, name, columnType, false, length) {
}

fun <T:Table> PKColumn<Int, T>.auto(): GeneratedPKColumn<Int, T> {
    (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
    val column = GeneratedPKColumn<Int, T>(table, name, columnType, length)
    (table.tableColumns as ArrayList<Column<*, T>>).add(column)
    return column
}

open class FKColumn<C, T: Table>(table: T, name: String, columnType: ColumnType, length: Int, val reference: Column<C, *>?) : Column<C, T>(table, name, columnType, true, length) {
}

class GeneratedPKColumn<C, T: Table>(table: T, name: String, columnType: ColumnType, length: Int) : PKColumn<C, T>(table, name, columnType, length), GeneratedValue<C> {
}