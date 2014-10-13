package kotlinx.nosql

abstract class KeyValueSchema(name: String): AbstractSchema(name) {

}

fun <T: KeyValueSchema, X> T.find(x: T.() -> X): X {
    return x()
}