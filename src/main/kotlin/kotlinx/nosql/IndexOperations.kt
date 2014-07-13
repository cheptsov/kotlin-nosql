package kotlinx.nosql

trait IndexOperations {
    fun createIndex(schema: AbstractSchema, index: AbstractIndex)
}