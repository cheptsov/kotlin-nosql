package kotlin.nosql

import java.util.ArrayList
import java.util.regex.Pattern

open class AbstractColumn<C, T : Schema, S>(val name: String, val valueClass: Class<S>, val columnType: ColumnType) : Field<C, T>() {
    fun matches(other: Pattern): Op {
        return MatchesOp(this, LiteralOp(other))
    }

    fun toString(): String {
        return "$name"
    }

    fun <C2> plus(c: AbstractColumn<C2, T, *>): Template2<T, C, C2> {
        return Template2(this, c) as Template2<T, C, C2>
    }
}

fun <T : Schema, C> AbstractColumn<C?, T, C>.isNull(): Op {
    return NullOp(this)
}

fun <T : Schema, C> AbstractColumn<C?, T, C>.isNotNull(): Op {
    return NotNullOp(this)
}

fun <T : Schema, C> AbstractColumn<out C?, T, *>.equal(other: C): Op {
    return EqualsOp(this, LiteralOp(other))
}

fun <T : Schema, C> AbstractColumn<out C?, T, *>.notEqual(other: C): Op {
    return NotEqualsOp(this, LiteralOp(other))
}

fun <T : Schema, C> AbstractColumn<out C?, T, *>.isMember(other: Iterable<C>): Op {
    return InOp(this, LiteralOp(other))
}

fun <T : Schema, C> AbstractColumn<out C?, T, *>.isMember(other: Array<C>): Op {
    return InOp(this, LiteralOp(other))
}

fun <T : Schema, C> AbstractColumn<out C?, T, *>.isMember(other: Expression<out Iterable<C>>): Op {
    return InOp(this, LiteralOp(other))
}

fun <T : Schema, C> AbstractColumn<out C?, T, *>.isNotMember(other: Iterable<C>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : Schema, C> AbstractColumn<out C?, T, *>.isNotMember(other: Array<C>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : Schema, C> AbstractColumn<out C?, T, *>.isNotMember(other: Expression<out Iterable<C>>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : Schema, C> AbstractColumn<out C?, T, C>.equal(other: Expression<out C?>): Op {
    return EqualsOp(this, other)
}

fun <T : Schema, C> AbstractColumn<out C?, T, C>.notEqual(other: Expression<out C?>): Op {
    return NotEqualsOp(this, other)
}

fun <T : Schema> AbstractColumn<out Int?, T, Int>.gt(other: Expression<out Int?>): Op {
    return GreaterOp(this, other)
}

fun <T : Schema> AbstractColumn<out Int?, T, Int>.gt(other: Int): Op {
    return GreaterOp(this, LiteralOp(other))
}

fun <T : Schema> AbstractColumn<out Int?, T, Int>.ge(other: Expression<out Int?>): Op {
    return GreaterEqualsOp(this, other)
}

fun <T : Schema> AbstractColumn<out Int?, T, Int>.ge(other: Int): Op {
    return GreaterEqualsOp(this, LiteralOp(other))
}

fun <T : Schema> AbstractColumn<out Int?, T, Int>.le(other: Expression<out Int?>): Op {
    return LessEqualsOp(this, other)
}

fun <T : Schema> AbstractColumn<out Int?, T, Int>.le(other: Int): Op {
    return LessEqualsOp(this, LiteralOp(other))
}

fun <T : Schema> AbstractColumn<out Int?, T, Int>.lt(other: Expression<out Int?>): Op {
    return LessOp(this, other)
}

fun <T : Schema> AbstractColumn<out Int?, T, Int>.lt(other: Int): Op {
    return LessOp(this, LiteralOp(other))
}


open class Column<C, T : Schema>(name: String, valueClass: Class<C>, columnType: ColumnType = ColumnType.CUSTOM_CLASS) : AbstractColumn<C, T, C>(name, valueClass, columnType) {
}

open class NullableColumn<C, T : Schema> (name: String, valueClass: Class<C>,
                                                  columnType: ColumnType) : AbstractColumn<C?, T, C>(name, valueClass, columnType) {
}

open class SetColumn<C, T : Schema> (name: String, valueClass: Class<C>) : AbstractColumn<Set<C>, T, C>(name, valueClass, ColumnType.CUSTOM_CLASS_SET) {
}

open class ListColumn<C, T : Schema> (name: String, valueClass: Class<C>) : AbstractColumn<List<C>, T, C>(name, valueClass, ColumnType.CUSTOM_CLASS_LIST) {
}

/*
TODO TODO TODO
class AggregatingFunction<C, T: Schema> (name: String, valueClass: Class<C>, columnType: ColumnType, val function: String) : AbstractColumn<C, T, C>(name, valueClass, columnType) {
}

fun <T: Schema> Count(column: AbstractColumn<*, T, *>): AggregatingFunction<Int, T> {
    return AggregatingFunction(column.name, javaClass<Int>(), ColumnType.INTEGER, "count")
}
*/

open class PrimaryKeyColumn<C, T : Schema>(table: T, name: String, valueClass: Class<C>, columnType: ColumnType) : AbstractColumn<C, T, C>(name, valueClass, columnType) {
}