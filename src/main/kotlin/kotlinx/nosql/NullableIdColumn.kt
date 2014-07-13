package kotlinx.nosql

open class NullableIdColumn<I, S : TableSchema<I>, R: TableSchema<I>> (name: String, valueClass: Class<I>,
                                                                       columnType: ColumnType) : AbstractColumn<Id<I, R>?, S, I>(name, valueClass, columnType), AbstractNullableColumn {
}