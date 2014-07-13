package kotlinx.nosql

open class ListColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>) : AbstractColumn<List<C>, S, C>(name, valueClass, ColumnType.CUSTOM_CLASS_LIST) {
}