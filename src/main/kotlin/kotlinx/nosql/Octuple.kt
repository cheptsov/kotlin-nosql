package kotlinx.nosql

class Octuple<A1, A2, A3, A4, A5, A6, A7, A8>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5, val a6: A6, val a7: A7, val a8: A8) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
    public fun component5(): A5 = a5
    public fun component6(): A6 = a6
    public fun component7(): A7 = a7
    public fun component8(): A8 = a8
}