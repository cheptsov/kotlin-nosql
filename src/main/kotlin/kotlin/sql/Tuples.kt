package kotlin.sql

class Quad<A, B, C, D>(val first: A, val second: B, val third: C, val fourth: D) {
    fun component1() = first
    fun component2() = second
    fun component3() = third
    fun component4() = fourth
}