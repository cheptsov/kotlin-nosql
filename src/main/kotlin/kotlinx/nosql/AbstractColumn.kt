package kotlinx.nosql

import java.util.ArrayList
import java.util.regex.Pattern
import kotlinx.nosql.query.*

open class AbstractColumn<C, T : AbstractSchema, S>(val name: String, val valueClass: Class<S>, val columnType: ColumnType) : ColumnQueryWrapper<C>(), Expression<C> {
    fun matches(other: Pattern): Query {
        return MatchesQuery(this, LiteralExpression(other))
    }

    override fun toString(): String {
        return "$name"
    }

    fun <C2> plus(c: AbstractColumn<C2, T, *>): ColumnPair<T, C, C2> {
        return ColumnPair(this, c) as ColumnPair<T, C, C2>
    }
}

fun <C, T: AbstractSchema> AbstractColumn<C, T, *>.update(value: C): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, array(wrapper.params.projection.get(0) to value),
            wrapper.params.query!!)
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.addAll(values: S): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().addAll(wrapper.params.table,
            wrapper.params.projection.get(0)
                    as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, values,
            wrapper.params.query!!)
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.add(value: C): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    val values: Collection<C> = if (wrapper.params.projection.get(0).columnType.list) listOf(value) else setOf(value)
    return Session.current<Session>().addAll(wrapper.params.table,
                    wrapper.params.projection.get(0)
                            as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, values,
                    wrapper.params.query!!)
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.removeAll(values: S): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().removeAll(wrapper.params.table,
            wrapper.params.projection.get(0)
                    as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, values,
            wrapper.params.query!!)
}

fun <C> AbstractColumn<out Collection<C>, *, *>.remove(value: C): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    val values: Collection<C> = if (wrapper.params.projection.get(0).columnType.list) listOf(value) else setOf(value)
    return Session.current<Session>().removeAll(wrapper.params.table,
            wrapper.params.projection.get(0)
                    as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, values,
            wrapper.params.query!!)
}

fun <C: AbstractColumn<out Collection<*>, *, *>> C.remove(removeOp: C.() -> Query): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
        val removeOpValue = with (wrapper.params.projection.get(0)) {
            removeOp()
        }
    return Session.current<Session>().removeAll(wrapper.params.table,
            wrapper.params.projection.get(0)
                    as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, removeOpValue,
            wrapper.params.query!!)
}

fun <T : AbstractSchema, C> AbstractColumn<C?, T, C>.isNull(): Query {
    return IsNullQuery(this)
}

fun <T : AbstractSchema, C> AbstractColumn<C?, T, C>.notNull(): Query {
    return IsNotNullQuery(this)
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.equal(other: C): Query {
    return EqualQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notEqual(other: C): Query {
    return NotEqualQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Iterable<C>): Query {
    return MemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Array<C>): Query {
    return MemberOfQuery(this, LiteralExpression(other))
}

// TODO TODO TODO: Expression should be typed
fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Expression<out Iterable<C>>): Query {
    return MemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Iterable<C>): Query {
    return NotMemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Array<C>): Query {
    return NotMemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Expression<out Iterable<C>>): Query {
    return NotMemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, C>.equal(other: Expression<out C?>): Query {
    return EqualQuery(this, other)
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, C>.notEqual(other: Expression<out C?>): Query {
    return NotEqualQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.gt(other: Expression<out Int?>): Query {
    return GreaterQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.gt(other: Int): Query {
    return GreaterQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.ge(other: Expression<out Int?>): Query {
    return GreaterEqualQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.ge(other: Int): Query {
    return GreaterEqualQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.le(other: Expression<out Int?>): Query {
    return LessEqualQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.le(other: Int): Query {
    return LessEqualQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.lt(other: Expression<out Int?>): Query {
    return LessQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.lt(other: Int): Query {
    return LessQuery(this, LiteralExpression(other))
}