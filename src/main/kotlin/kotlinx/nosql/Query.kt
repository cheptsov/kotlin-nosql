package kotlinx.nosql

import kotlinx.nosql.query.OrQuery

abstract class Query() {
    infix fun and(op: Query): Query {
        return AndQuery(this, op)
    }

    infix fun or(op: Query): Query {
        return OrQuery(this, op)
    }
}