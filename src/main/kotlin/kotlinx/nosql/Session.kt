package kotlinx.nosql

import java.util.ArrayList

interface Session {
    fun <T : AbstractSchema>T.create()

    fun <T : AbstractSchema>T.drop()

    // TODO: Refactor
    fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<*, T, *>, *>>)

    // TODO: Refactor
    fun <T : AbstractSchema> delete(table: T, op: Query): Int

    fun update(schema: AbstractSchema, columnValues: Array<Pair<AbstractColumn<*, *, *>, *>>, op: Query): Int

    fun <T> addAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Query): Int
    fun <T: Number> incr(schema: KeyValueSchema, column: AbstractColumn<*, *, T>, value: T): T
    //internal fun <T: Number> incr(schema: AbstractSchema, column: AbstractColumn<*, *, T>, value: T, op: Query): T

    fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Query): Int

    fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, removeOp: Query, op: Query): Int

    companion object {
        val threadLocale = ThreadLocal<Session>()

        fun <T> current(): T {
            return threadLocale.get()!! as T
        }
    }
}
