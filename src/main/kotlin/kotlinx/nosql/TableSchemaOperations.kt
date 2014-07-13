package kotlinx.nosql

trait TableSchemaOperations {
    internal fun <T : TableSchema<P>, P, V> find(params: TableSchemaProjectionQueryParams<T, P, V>): Iterator<V>
}