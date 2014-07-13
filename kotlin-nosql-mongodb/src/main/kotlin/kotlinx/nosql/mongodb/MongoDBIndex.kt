package kotlinx.nosql.mongodb

import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.AbstractIndex

class MongoDBIndex(name: String = "", unique: Boolean = false, val ascending: Array<out AbstractColumn<*, *, *>>,
                   val descending: Array<out AbstractColumn<*, *, *>>,
                   val text: Array<out AbstractColumn<*, *, String>>) : AbstractIndex(name)