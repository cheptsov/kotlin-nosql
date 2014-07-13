package kotlinx.nosql

class TableSchemaProjectionQueryParams<T : TableSchema<P>, P, V>(val table: T, val projection: List<AbstractColumn<*, *, *>>, val query: Query? = null,
                                                                 var skip: Int? = null, var take: Int? = null) {
}