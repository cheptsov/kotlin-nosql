package kotlin.nosql

import java.util.ArrayList

open class AbstractColumn<C, T : AbstractSchema, S>(val name: String, val valueClass: Class<S>, val columnType: ColumnType) : Field<C, T>() {
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
        return "$name"
    }

    fun <C2> plus(c: AbstractColumn<C2, T, *>): Template2<T, C, C2> {
        return Template2(this, c) as Template2<T, C, C2>
    }
}

open class Column<C, T : AbstractSchema>(name: String, valueClass: Class<C>, columnType: ColumnType = ColumnType.CUSTOM_CLASS) : AbstractColumn<C, T, C>(name, valueClass, columnType) {
}

open class NullableColumn<C, T : AbstractSchema> (name: String, valueClass: Class<C>,
                                                  columnType: ColumnType) : AbstractColumn<C?, T, C>(name, valueClass, columnType) {
}

open class SetColumn<C, T : AbstractSchema> (name: String, valueClass: Class<C>,
                                             columnType: ColumnType) : AbstractColumn<Set<C>, T, C>(name, valueClass, columnType) {
}

open class ListColumn<C, T : AbstractSchema> (name: String, valueClass: Class<C>, columnType: ColumnType) : AbstractColumn<List<C>, T, C>(name, valueClass, columnType) {
}

val AbstractColumn<*, *, *>.isNull: Op
    get() {
        return IsNullOp(this)
    }

fun AbstractColumn<*, *, *>.eq(other: Expression): Op {
    return EqualsOp(this, other)
}

open class PKColumn<C, T : AbstractSchema>(table: T, name: String, valueClass: Class<C>, columnType: ColumnType) : AbstractColumn<C, T, C>(name, valueClass, columnType) {
}