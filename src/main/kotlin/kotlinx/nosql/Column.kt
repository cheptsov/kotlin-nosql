package kotlinx.nosql

abstract class Column<C, S : AbstractSchema>(name: String, valueClass: Class<C>, columnType: ColumnType = ColumnType.CUSTOM_CLASS) : AbstractColumn<C, S, C>(name, valueClass, columnType) {
}