package kotlinx.nosql

abstract class Database<S: Session>(val schemas: Array<AbstractSchema>) {
    abstract fun withSession(statement: S.() -> Unit)
}
