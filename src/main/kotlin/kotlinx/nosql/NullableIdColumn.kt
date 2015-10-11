package kotlinx.nosql

import kotlin.reflect.KClass

open class NullableIdColumn<I: Any, S : TableSchema<I>, R: TableSchema<I>> (name: String, valueClass: KClass<I>,
                                                                            columnType: ColumnType) : AbstractColumn<Id<I, R>?, S, I>(name, valueClass, columnType), AbstractNullableColumn {
}