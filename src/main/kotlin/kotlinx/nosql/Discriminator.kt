package kotlinx.nosql

class Discriminator<D: Any, S : DocumentSchema<out Any, out Any>>(val column: AbstractColumn<D, S, D>, val value: D) {
}