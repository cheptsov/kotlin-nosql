package kotlinx.nosql

import java.util.ArrayList

trait Session {
    fun <T : AbstractSchema>T.create()

    fun <T : AbstractSchema>T.drop()

    // TODO: Refactor
    internal fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<*, T, *>, *>>)

    // TODO: Refactor
    internal fun <T : AbstractSchema> delete(table: T, op: Query): Int

    internal fun update(schema: AbstractSchema, columnValues: Array<Pair<AbstractColumn<*, *, *>, *>>, op: Query): Int

    internal fun <T> addAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Query): Int

    internal fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Query): Int

    internal fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, removeOp: Query, op: Query): Int

    class object {
        val threadLocale = ThreadLocal<Session>()

        fun <T> current(): T {
            return threadLocale.get()!! as T
        }
    }
}
