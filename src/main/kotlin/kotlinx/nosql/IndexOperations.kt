package kotlinx.nosql

interface IndexOperations {
    fun createIndex(schema: AbstractSchema, index: AbstractIndex)
}