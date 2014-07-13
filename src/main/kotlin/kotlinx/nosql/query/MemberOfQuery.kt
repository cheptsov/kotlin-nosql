package kotlinx.nosql.query

import kotlinx.nosql.Query
import kotlinx.nosql.Expression

class MemberOfQuery(val expr1: Expression<*>, val expr2: Expression<*>): Query() {
}
