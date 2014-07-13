package kotlinx.nosql

open class NullableColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>,
                                                  columnType: ColumnType) : AbstractColumn<C?, S, C>(name, valueClass, columnType), AbstractNullableColumn {
}