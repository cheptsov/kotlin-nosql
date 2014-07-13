package kotlinx.nosql

open class SetColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>) : AbstractColumn<Set<C>, S, C>(name, valueClass, ColumnType.CUSTOM_CLASS_SET) {
}