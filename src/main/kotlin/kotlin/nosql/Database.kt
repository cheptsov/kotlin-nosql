package kotlin.nosql

abstract class Database<S: Session>(val schemas: Array<AbstractSchema>) {
    abstract fun invoke(statement: S.() -> Unit)
}
