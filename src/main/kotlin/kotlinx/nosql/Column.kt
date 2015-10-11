package kotlinx.nosql

import kotlin.reflect.KClass

abstract class Column<C: Any, S : AbstractSchema>(name: String, valueClass: KClass<C>, columnType: ColumnType = ColumnType.CUSTOM_CLASS) : AbstractColumn<C, S, C>(name, valueClass, columnType) {
}