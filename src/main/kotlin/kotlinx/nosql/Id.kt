package kotlinx.nosql

class Id<I, R: TableSchema<I>>(val value: I) {
    override fun toString() = value.toString()

    override fun equals(other: Any?): Boolean {
        return if (other is Id<*, *>) value.equals((other as Id<*, *>).value) else false
    }
    override fun hashCode(): Int {
        return value.hashCode()
    }
}