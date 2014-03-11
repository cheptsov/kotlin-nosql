package kotlinx.nosql

abstract class Database<S: Session>(val schemas: Array<Schema>) {
    abstract fun invoke(statement: S.() -> Unit)
}
