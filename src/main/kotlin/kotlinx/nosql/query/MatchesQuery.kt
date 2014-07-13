package kotlinx.nosql.query

import kotlinx.nosql.Expression
import kotlinx.nosql.Query

class MatchesQuery(val expr1: Expression<*>, val expr2: Expression<*>): Query() {
}
