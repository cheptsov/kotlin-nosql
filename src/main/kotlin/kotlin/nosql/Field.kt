package kotlin.nosql

abstract class Field<C, T: Schema>(val table: T) : Expression {
}