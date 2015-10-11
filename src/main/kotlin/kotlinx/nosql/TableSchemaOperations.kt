package kotlinx.nosql

interface TableSchemaOperations {
    fun <T : TableSchema<P>, P: Any, V: Any> find(params: TableSchemaProjectionQueryParams<T, P, V>): Iterator<V>
}