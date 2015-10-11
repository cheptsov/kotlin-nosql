package kotlinx.nosql

open class PrimaryKey<I>(val name: String, val javaClass: Class<I>, val columnType: ColumnType) {
    companion object {
        fun string(name: String) = PrimaryKey<String>(name, String::class.java, ColumnType.STRING)
        fun integer(name: String) = PrimaryKey<Int>(name, Int::class.java, ColumnType.INTEGER)
    }
}