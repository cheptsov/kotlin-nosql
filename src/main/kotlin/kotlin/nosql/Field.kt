package kotlin.nosql

abstract class Field<C, T: Table>(val table: T) : Expression {
}