package kotlinx.nosql

open class PrimaryKey<I>(val name: String, val javaClass: Class<I>, val columnType: ColumnType) {
    class object {
        fun string(name: String) = PrimaryKey<String>(name, javaClass<String>(), ColumnType.STRING)
        fun integer(name: String) = PrimaryKey<Int>(name, javaClass<Int>(), ColumnType.INTEGER)
    }
}