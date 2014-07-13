package kotlinx.nosql

class TableSchemaProjectionQueryWrapper<T : TableSchema<P>, P, V>(val params: TableSchemaProjectionQueryParams<T, P, V>): Iterable<V> {
    override fun iterator(): Iterator<V> {
        return Session.current<TableSchemaOperations>().find(params)
    }

    fun skip(num: Int): TableSchemaProjectionQueryWrapper<T, P, V> {
        params.skip = num
        return this
    }

    fun take(num: Int): TableSchemaProjectionQueryWrapper<T, P, V> {
        params.take = num
        return this
    }

    class object {
        val threadLocal = ThreadLocal<TableSchemaProjectionQueryWrapper<out TableSchema<*>, *, *>>()

        fun get(): TableSchemaProjectionQueryWrapper<out TableSchema<*>, *, *> {
            return threadLocal.get()!! as TableSchemaProjectionQueryWrapper<out TableSchema<*>, *, *>
        }

        fun set(value: TableSchemaProjectionQueryWrapper<out TableSchema<*>, *, *>) {
            threadLocal.set(value)
        }
    }
}