package kotlinx.nosql.mongodb

import kotlinx.nosql.DocumentSchema
import kotlinx.nosql.string
import kotlinx.nosql.Discriminator
import kotlinx.nosql.AbstractSchema
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.ColumnType
import java.util.ArrayList

/*import kotlinx.nosql.AbstractDocument*/

abstract class Schema<D>(name: String, valueClass: Class<D>, discriminator: Discriminator<out Any, out DocumentSchema<String, D>>? = null) : DocumentSchema<String, D>(name, valueClass, string("_id"), discriminator) {
    internal val indices = ArrayList<Index>()

    internal fun ensureIndex(name: String = "", unique: Boolean = false,
                             ascending: Array<out AbstractColumn<*, *, *>> = array<AbstractColumn<*, *, *>>(),
                             descending: Array<out AbstractColumn<*, *, *>> = array<AbstractColumn<*, *, *>>(),
                             text: Array<out AbstractColumn<*, *, String>> = array<AbstractColumn<*, *, String>>()) {
        indices.add(Index(name, unique, ascending, descending, text))
    }
    /*internal fun ensureIndex(name: String = "", unique: Boolean = false, columns: Array<out AbstractColumn<*, *, *>>) {
        indices.add(Index(name, unique, columns, array<AbstractColumn<*, *, *>>(), array<AbstractColumn<*, *, String>>()))
    }*/
}

abstract class AbstractIndex(val name: String)

class Index(name: String = "", unique: Boolean = false, val ascending: Array<out AbstractColumn<*, *, *>>,
            val descending: Array<out AbstractColumn<*, *, *>>,
            val text: Array<out AbstractColumn<*, *, String>>) : AbstractIndex(name)

/*
abstract class Document<S: DocumentSchema<String, AbstractDocument<String, S>>> : AbstractDocument<String, S>() {

}
*/
