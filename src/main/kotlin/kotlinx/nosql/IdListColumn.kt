package kotlinx.nosql

open class IdListColumn<S : TableSchema<P>, R: TableSchema<P>, P: Any>  (name: String, val refSchema: R) : AbstractColumn<List<Id<P, R>>, S, Id<P, R>>(name, Id::class.java as Class<Id<P, R>>, ColumnType.ID_LIST) {
}
