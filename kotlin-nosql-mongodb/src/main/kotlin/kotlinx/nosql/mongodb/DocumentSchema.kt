package kotlinx.nosql.mongodb

import kotlinx.nosql.string
import kotlinx.nosql.Discriminator
import kotlinx.nosql.AbstractSchema
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.ColumnType
import java.util.ArrayList
import kotlinx.nosql.AbstractIndex

abstract class DocumentSchema<D>(name: String, valueClass: Class<D>, discriminator: Discriminator<out Any, out kotlinx.nosql.DocumentSchema<String, D>>? = null) : kotlinx.nosql.DocumentSchema<String, D>(name, valueClass, string("_id"), discriminator) {
    internal fun ensureIndex(name: String = "", unique: Boolean = false,
                             ascending: Array<out AbstractColumn<*, *, *>> = array<AbstractColumn<*, *, *>>(),
                             descending: Array<out AbstractColumn<*, *, *>> = array<AbstractColumn<*, *, *>>(),
                             text: Array<out AbstractColumn<*, *, String>> = array<AbstractColumn<*, *, String>>()) {
        indices.add(MongoDBIndex(name, unique, ascending, descending, text))
    }
}