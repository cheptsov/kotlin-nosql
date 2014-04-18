package kotlinx.nosql

import java.util.ArrayList
import java.util.regex.Pattern

open class AbstractColumn<C, T : AbstractSchema, S>(val name: String, val valueClass: Class<S>, val columnType: ColumnType) : Field<C, T>() {
    fun matches(other: Pattern): Op {
        return MatchesOp(this, LiteralOp(other))
    }

    override fun toString(): String {
        return "$name"
    }

    fun <C2> plus(c: AbstractColumn<C2, T, *>): Template2<T, C, C2> {
        return Template2(this, c) as Template2<T, C, C2>
    }
}

fun <T : AbstractSchema, C> AbstractColumn<C?, T, C>.isNull(): Op {
    return NullOp(this)
}

fun search(text: String): Op {
    return SearchOp(text)
}

fun <T : AbstractSchema, C> AbstractColumn<C?, T, C>.notNull(): Op {
    return NotNullOp(this)
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.equal(other: C): Op {
    return EqualsOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notEqual(other: C): Op {
    return NotEqualsOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Iterable<C>): Op {
    return InOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Array<C>): Op {
    return InOp(this, LiteralOp(other))
}

// TODO TODO TODO: Expression should be typed
fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Expression<out Iterable<C>>): Op {
    return InOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Iterable<C>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Array<C>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Expression<out Iterable<C>>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, C>.equal(other: Expression<out C?>): Op {
    return EqualsOp(this, other)
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, C>.notEqual(other: Expression<out C?>): Op {
    return NotEqualsOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.gt(other: Expression<out Int?>): Op {
    return GreaterOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.gt(other: Int): Op {
    return GreaterOp(this, LiteralOp(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.ge(other: Expression<out Int?>): Op {
    return GreaterEqualsOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.ge(other: Int): Op {
    return GreaterEqualsOp(this, LiteralOp(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.le(other: Expression<out Int?>): Op {
    return LessEqualsOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.le(other: Int): Op {
    return LessEqualsOp(this, LiteralOp(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.lt(other: Expression<out Int?>): Op {
    return LessOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.lt(other: Int): Op {
    return LessOp(this, LiteralOp(other))
}

abstract class Column<C, S : AbstractSchema>(name: String, valueClass: Class<C>, columnType: ColumnType = ColumnType.CUSTOM_CLASS) : AbstractColumn<C, S, C>(name, valueClass, columnType) {
}

trait AbstractNullableColumn {
}

open class NullableIdColumn<I, S : TableSchema<I>, R: TableSchema<I>> (name: String, valueClass: Class<I>,
                                                  columnType: ColumnType) : AbstractColumn<Id<I, R>?, S, I>(name, valueClass, columnType), AbstractNullableColumn {
}

open class NullableColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>,
                                                  columnType: ColumnType) : AbstractColumn<C?, S, C>(name, valueClass, columnType), AbstractNullableColumn {
}

open class SetColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>) : AbstractColumn<Set<C>, S, C>(name, valueClass, ColumnType.CUSTOM_CLASS_SET) {
}

open class ListColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>) : AbstractColumn<List<C>, S, C>(name, valueClass, ColumnType.CUSTOM_CLASS_LIST) {
}

/*
TODO TODO TODO
class AggregatingFunction<C, T: Schema> (name: String, valueClass: Class<C>, columnType: ColumnType, val function: String) : AbstractColumn<C, T, C>(name, valueClass, columnType) {
}

fun <T: Schema> Count(column: AbstractColumn<*, T, *>): AggregatingFunction<Int, T> {
    return AggregatingFunction(column.name, javaClass<Int>(), ColumnType.INTEGER, "count")
}
*/