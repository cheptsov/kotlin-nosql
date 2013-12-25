package kotlin.nosql

trait Expression {
    fun toSQL(): String
}