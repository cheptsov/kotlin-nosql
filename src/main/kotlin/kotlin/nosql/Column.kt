package kotlin.nosql

import java.util.ArrayList

open class Column<C, T : AbstractSchema>(table: T, val name: String, val columnType: ColumnType, val _nullable: Boolean) : Field<C, T>(table) {
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
        return "${table.name}.$name"
    }

    fun invoke(av: C): Array<Pair<Column<*, T>, *>> {
        return array(Pair(this, av))
    }

    fun <C2> plus(c: Column<C2, T>): Template2<T, C, C2> {
        return Template2(table, this, c) as Template2<T, C, C2>
    }
}

fun <C, T: TableSchema> Column<C, T>.PrimaryKey(): PKColumn<C, T> {
    (table.columns as ArrayList<Column<*, T>>).remove(this)
    val column = PKColumn<C, T>(table, name, columnType)
    (table.columns as ArrayList<Column<*, T>>).add(column)
    (table.primaryKeys as ArrayList<PKColumn<*, T>>).add(column)
    return column
}



fun <C, T : AbstractSchema> Column<C, T>.Nullable(): Column<C?, T> {
    (table.columns as ArrayList<Column<*, T>>).remove(this)
    val column = (Column<C?, T>(table, name, columnType, true)) as Column<C?, T>
    (table.columns as ArrayList<Column<*, T>>).add(column)
    return column
}

fun <C, T : AbstractSchema> Column<C, T>.Set(): Column<Set<C>, T> {
    (table.columns as ArrayList<Column<*, T>>).remove(this)
    val column = (Column<Set<C>, T>(table, name,
            when (columnType) { ColumnType.INTEGER -> ColumnType.INTEGER_SET
                ColumnType.STRING -> ColumnType.STRING_SET
                else -> throw IllegalArgumentException()
            }, true)) as Column<Set<C>, T>
    (table.columns as ArrayList<Column<*, T>>).add(column)
    return column
}

fun <C, T : AbstractSchema> Column<C, T>.List(): Column<List<C>, T> {
    (table.columns as ArrayList<Column<*, T>>).remove(this)
    val column = (Column<List<C>, T>(table, name,
            when (columnType) { ColumnType.INTEGER -> ColumnType.INTEGER_LIST
                ColumnType.STRING -> ColumnType.STRING_LIST
                else -> throw IllegalArgumentException()
            }, true)) as Column<List<C>, T>
    (table.columns as ArrayList<Column<*, T>>).add(column)
    return column
}

val Column<*, *>.isNull: Op
    get() {
        return IsNullOp(this)
    }

fun Column<*, *>.eq(other: Expression): Op {
    return EqualsOp(this, other)
}

open class PKColumn<C, T : AbstractSchema>(table: T, name: String, attributeType: ColumnType) : Column<C, T>(table, name, attributeType, false) {
}