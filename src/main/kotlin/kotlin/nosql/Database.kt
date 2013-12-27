package kotlin.nosql

abstract class Database<S: Session>() {
    abstract fun invoke(statement: S.() -> Unit)
}
