package kotlinx.nosql

open class IdListColumn<S : TableSchema<P>, R: TableSchema<P>, P>  (name: String, val refSchema: R) : AbstractColumn<List<Id<P, R>>, S, Id<P, R>>(name, javaClass<Id<P, R>>(), ColumnType.ID_LIST) {
}
