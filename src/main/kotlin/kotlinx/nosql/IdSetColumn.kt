package kotlinx.nosql

import kotlin.reflect.KClass

open class IdSetColumn<S : TableSchema<P>, R: TableSchema<P>, P: Any>  (name: String, val refSchema: R) : AbstractColumn<Set<Id<P, R>>, S, Id<P, R>>(name, Id::class as KClass<Id<P, R>>, ColumnType.ID_SET) {
}
