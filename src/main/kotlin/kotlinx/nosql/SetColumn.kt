package kotlinx.nosql

import kotlin.reflect.KClass

open class SetColumn<C: Any, S : AbstractSchema> (name: String, valueClass: KClass<C>) : AbstractColumn<Set<C>, S, C>(name, valueClass, ColumnType.CUSTOM_CLASS_SET) {
}