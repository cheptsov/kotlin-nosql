package kotlinx.nosql

import kotlinx.nosql.query.OrQuery

abstract class Query() {
    fun and(op: Query): Query {
        return AndQuery(this, op)
    }

    fun or(op: Query): Query {
        return OrQuery(this, op)
    }
}