package kotlin.sql

import java.util.ArrayList

open class Column<C, T: Table>(val table: T, val name: String, val columnType: ColumnType, val _nullable: Boolean, val length: Int, val autoIncrement: Boolean, val references: Column<*, *>?) : Field<C>() {
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

    fun <T2: Table, C2> times(c2: Column<C2, T2>): Template11<T, C, T2, C2> {
        return Template11<T, C, T2, C2>(table, this, c2.table, c2)
    }

    val primaryKey: PKColumn<C, T>
        get() {
            (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
            val column = PKColumn<C, T>(table, name, columnType, length, autoIncrement, references)
            (table.tableColumns as ArrayList<Column<*, T>>).add(column)
            (table.primaryKeys as ArrayList<PKColumn<*, T>>).add(column)
            return column
        }

    val nullable: Column<C?, T>
        get() {
            (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
            val column = (Column<C?, T>(table, name, columnType, true, length, autoIncrement, references)) as Column<C?, T>
            (table.tableColumns as ArrayList<Column<*, T>>).add(column)
            return column
        }
}

fun Column<*, *>.isNull(): Op {
    return IsNullOp(this)
}

fun Column<*, *>.equals(other: Expression): Op {
    return EqualsOp(this, other)
}

open class PKColumn<C, T: Table>(table: T, name: String, columnType: ColumnType, length: Int, autoIncrement: Boolean, references: Column<*, *>?) : Column<C, T>(table, name, columnType, false, length, autoIncrement, references) {
    val auto: GeneratedPKColumn<C, T>
        get() {
            (table.tableColumns as ArrayList<Column<*, T>>).remove(this)
            val column = GeneratedPKColumn<C, T>(table, name, columnType, length, autoIncrement = true, references = references)
            (table.tableColumns as ArrayList<Column<*, T>>).add(column)
            return column
        }
}



class GeneratedPKColumn<C, T: Table>(table: T, name: String, columnType: ColumnType, length: Int, autoIncrement: Boolean, references: Column<*, *>?) : PKColumn<C, T>(table, name, columnType, length, autoIncrement, references), GeneratedValue<C> {
}