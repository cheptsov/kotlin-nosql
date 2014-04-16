package kotlinx.nosql

abstract class Op() {
    fun and(op: Op): Op {
        return AndOp(this, op)
    }

    fun or(op: Op): kotlinx.nosql.Op {
        return kotlinx.nosql.OrOp(this, op)
    }
}

object NoOp : Op() {
}

class NullOp(val column: AbstractColumn<*, *, *>): Op() {
}

class NotNullOp(val column: AbstractColumn<*, *, *>): Op() {
}

class LiteralOp(val value: Any): Expression<Any> {
}

class EqualsOp(val expr1: Expression<*>, val expr2: Expression<*>): Op() {
}

class NotEqualsOp(val expr1: Expression<*>, val expr2: Expression<*>): Op() {
}

class GreaterOp(val expr1: Expression<*>, val expr2: Expression<*>): kotlinx.nosql.Op() {
}

class GreaterEqualsOp(val expr1: Expression<*>, val expr2: Expression<*>): Op() {
}

class LessOp(val expr1: Expression<*>, val expr2: Expression<*>): Op() {
}

class LessEqualsOp(val expr1: Expression<*>, val expr2: Expression<*>): kotlinx.nosql.Op() {
}

class InOp(val expr1: Expression<*>, val expr2: Expression<*>): Op() {
}

class NotInOp(val expr1: Expression<*>, val expr2: Expression<*>): Op() {
}

class MatchesOp(val expr1: Expression<*>, val expr2: Expression<*>): kotlinx.nosql.Op() {
}

class AndOp(val expr1: kotlinx.nosql.Op, val expr2: kotlinx.nosql.Op): Op() {
}

class OrOp(val expr1: kotlinx.nosql.Op, val expr2: Op): Op() {
}