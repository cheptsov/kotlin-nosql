package kotlinx.nosql

class Discriminator<D, S : DocumentSchema<out Any, out Any>>(val column: AbstractColumn<D, S, D>, val value: D) {
}