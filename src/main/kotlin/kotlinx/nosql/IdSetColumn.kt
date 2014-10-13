package kotlinx.nosql

open class IdSetColumn<S : TableSchema<P>, R: TableSchema<P>, P>  (name: String, val refSchema: R) : AbstractColumn<Set<Id<P, R>>, S, Id<P, R>>(name, javaClass<Id<P, R>>(), ColumnType.ID_SET) {
}
