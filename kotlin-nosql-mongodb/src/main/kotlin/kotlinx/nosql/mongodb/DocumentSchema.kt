package kotlinx.nosql.mongodb

import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.Discriminator
import kotlinx.nosql.string
import kotlin.reflect.KClass

abstract class DocumentSchema<D: Any>(name: String, valueClass: KClass<D>, discriminator: Discriminator<out Any, out kotlinx.nosql.DocumentSchema<String, D>>? = null) : kotlinx.nosql.DocumentSchema<String, D>(name, valueClass, string("_id"), discriminator) {
    fun ensureIndex(name: String = "", unique: Boolean = false,
                             ascending: Array<out AbstractColumn<*, *, *>> = arrayOf<AbstractColumn<*, *, *>>(),
                             descending: Array<out AbstractColumn<*, *, *>> = arrayOf<AbstractColumn<*, *, *>>(),
                             text: Array<out AbstractColumn<*, *, String>> = arrayOf<AbstractColumn<*, *, String>>()) {
        indices.add(MongoDBIndex(name, unique, ascending, descending, text))
    }
}