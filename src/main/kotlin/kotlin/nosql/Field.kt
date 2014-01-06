package kotlin.nosql

abstract class Field<C, T: AbstractSchema>(val table: T) : Expression {
}