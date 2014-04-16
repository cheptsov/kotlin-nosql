package kotlinx.nosql.mongodb

import kotlinx.nosql.DocumentSchema
import kotlinx.nosql.string
import kotlinx.nosql.Discriminator
/*import kotlinx.nosql.AbstractDocument*/

abstract class Schema<D>(name: String, valueClass: Class<D>, discriminator: Discriminator<out Any, out DocumentSchema<String, D>>? = null) : DocumentSchema<String, D>(name, valueClass, string("_id"), discriminator) {
}

/*
abstract class Document<S: DocumentSchema<String, AbstractDocument<String, S>>> : AbstractDocument<String, S>() {

}
*/
