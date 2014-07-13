package kotlinx.nosql.query

import kotlinx.nosql.Query

class OrQuery(val expr1: Query, val expr2: Query): Query() {
}