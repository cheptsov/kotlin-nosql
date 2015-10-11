package kotlinx.nosql

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

abstract class DocumentSchema<I: Any, D: Any>(name: String, val valueClass: KClass<D>, primaryKey: AbstractColumn<I,
        out DocumentSchema<I, D>, I>, val discriminator: Discriminator<out Any, out DocumentSchema<out Any, out Any>>? = null) : TableSchema<I>(name, primaryKey) {
    init {
        if (discriminator != null) {
            val emptyDiscriminators = CopyOnWriteArrayList<Discriminator<*, *>>()
            val discriminators = tableDiscriminators.putIfAbsent(name, emptyDiscriminators)
            if (discriminators != null)
                discriminators.add(discriminator)
            else
                emptyDiscriminators.add(discriminator)
            // TODO TODO TODO
            discriminatorClasses.put(discriminator, this.valueClass)
            discriminatorSchemaClasses.put(discriminator, this.javaClass)
            discriminatorSchemas.put(discriminator, this)
        }
    }

    companion object {
        val tableDiscriminators = ConcurrentHashMap<String, MutableList<Discriminator<*, *>>>()
        val discriminatorClasses = ConcurrentHashMap<Discriminator<*, *>, KClass<*>>()
        val discriminatorSchemaClasses = ConcurrentHashMap<Discriminator<*, *>, Class<*>>()
        val discriminatorSchemas = ConcurrentHashMap<Discriminator<*, *>, AbstractSchema>()
    }
}