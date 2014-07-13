package kotlinx.nosql.query

import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.Query

class IsNotNullQuery(val column: AbstractColumn<*, *, *>): Query() {
}