package kotlinx.nosql.query

import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.Query

class IsNullQuery(val column: AbstractColumn<*, *, *>): Query() {
}