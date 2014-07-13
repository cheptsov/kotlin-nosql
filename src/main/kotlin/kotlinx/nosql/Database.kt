package kotlinx.nosql

import java.util.concurrent.ConcurrentHashMap
import kotlinx.nosql.util.getAllFields
import kotlinx.nosql.util.isColumn
import kotlinx.nosql.util.asColumn

abstract class Database<S: Session>(val schemas: Array<out AbstractSchema>, val initialization: DatabaseInitialization<S>) {
    abstract fun <R> withSession(statement: S.() -> R): R

    fun initialize() {
        for (schema in schemas) {
            buildFullColumnNames(schema)
            when (initialization) {
            // TODO: implement validation
                is Create, is CreateDrop -> {
                    withSession {
                        schema.drop()
                        schema.create()
                        if (this is IndexOperations)
                            for (index in schema.indices)
                                createIndex(schema, index)
                    }
                }
                is Update -> {
                    withSession {
                        if (this is IndexOperations)
                            for (index in schema.indices)
                                createIndex(schema, index)
                    }
                }
            // TODO: implement drop after exit
            }
        }
        withSession {
            if (initialization is Create) {
                initialization.onCreate()
            } else if (initialization is CreateDrop) {
                initialization.onCreate();
            }
        }
    }

    private fun buildFullColumnNames(schema: Any, path: String = "") {
        val fields = getAllFields(schema.javaClass)
        for (field in fields) {
            if (field.isColumn) {
                val column = field.asColumn(schema)
                val columnFullName = path + (if (path.isNotEmpty()) "." else "") + column.name
                fullColumnNames.put(column, columnFullName)
                buildFullColumnNames(column, columnFullName)
            }
        }
    }

    class object {
        val fullColumnNames = ConcurrentHashMap<AbstractColumn<*, *, *>, String>()
    }
}

val AbstractColumn<*, *, *>.fullName: String
    get() {
        return Database.fullColumnNames.get(this)!!
    }
