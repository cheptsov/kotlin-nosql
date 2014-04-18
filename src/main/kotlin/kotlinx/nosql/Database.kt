package kotlinx.nosql

abstract class Database<S: Session>(val schemas: Array<out AbstractSchema>, initialDatabaseSetup: DatabaseInitialization<S>) {
    abstract fun withSession(statement: S.() -> Unit)
}
