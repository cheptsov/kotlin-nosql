package kotlin.nosql

class NotFoundException(val key: Any) : RuntimeException("Key not found: $key") {

}