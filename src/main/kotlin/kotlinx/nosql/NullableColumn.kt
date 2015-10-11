package kotlinx.nosql

import kotlin.reflect.KClass

open class NullableColumn<C: Any, S : AbstractSchema> (name: String, valueClass: KClass<C>,
                                                  columnType: ColumnType) : AbstractColumn<C?, S, C>(name, valueClass, columnType), AbstractNullableColumn {
}