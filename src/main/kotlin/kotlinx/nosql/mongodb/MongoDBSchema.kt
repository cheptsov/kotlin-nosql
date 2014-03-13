package kotlinx.nosql.mongodb

import kotlinx.nosql.DocumentSchema
import kotlinx.nosql.string
import kotlinx.nosql.Discriminator

abstract class MongoDBSchema<V>(name: String, valueClass: Class<V>, discriminator: Discriminator<out Any, out DocumentSchema<String, V>>? = null) : DocumentSchema<String, V>(name, valueClass, string("_id"), discriminator) {

}

