package kotlinx.nosql

import kotlin.reflect.KClass

open class ListColumn<C: Any, S : AbstractSchema> (name: String, valueClass: KClass<C>) : AbstractColumn<List<C>, S, C>(name, valueClass, ColumnType.CUSTOM_CLASS_LIST) {
}