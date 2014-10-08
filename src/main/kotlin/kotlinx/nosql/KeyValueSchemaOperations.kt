package kotlinx.nosql

trait KeyValueSchemaOperations {
    fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, *>): C
    //fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, *>): Int
    fun <T : KeyValueSchema, R : TableSchema<P>, P> T.next(c: T.() -> AbstractColumn<Id<P, R>, T, *>): Id<P, R>
    fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, *>, v: C)
}
