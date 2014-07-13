package kotlinx.nosql

abstract class TableSchema<I>(tableName: String, primaryKey: AbstractColumn<I, out TableSchema<I>, I>): AbstractTableSchema(tableName) {
    val pk = AbstractColumn<Id<I, TableSchema<I>>, TableSchema<I>, I>(primaryKey.name, primaryKey.valueClass, ColumnType.PRIMARY_ID)
}

// Extension functions

val <C, T : TableSchema<C>> T.id: AbstractColumn<Id<C, T>, T, C>
    get () {
        return pk as AbstractColumn<Id<C, T>, T, C>
    }

fun <S : TableSchema<P>, R: TableSchema<P>, P> id(name: String, schema: R): AbstractColumn<Id<P, R>, S, P> = AbstractColumn(name, schema.id.valueClass, ColumnType.FOREIGN_ID)
fun <S : TableSchema<P>, R: TableSchema<P>, P> S.id(name: String, schema: R): AbstractColumn<Id<P, R>, S, P> = AbstractColumn(name, schema.id.valueClass, ColumnType.FOREIGN_ID)

fun <S : TableSchema<P>, R: TableSchema<P>, P>  listOfId(name: String, schema: R): AbstractColumn<List<Id<P, R>>, S, Id<P, R>> = AbstractColumn(name, javaClass<Id<P, R>>(), ColumnType.ID_LIST)
fun <S : TableSchema<P>, R: TableSchema<P>, P> S.listOfId(name: String, schema: R): AbstractColumn<List<Id<P, R>>, S, Id<P, R>> = AbstractColumn(name, javaClass<Id<P, R>>(), ColumnType.ID_LIST)

fun <S : TableSchema<P>, R: TableSchema<P>, P>  setOfId(name: String, schema: R): AbstractColumn<Set<Id<P, R>>, S, Id<P, R>> = AbstractColumn(name, javaClass<Id<P, R>>(), ColumnType.ID_SET)
fun <S : TableSchema<P>, R: TableSchema<P>, P> S.setOfId(name: String, schema: R): AbstractColumn<Set<Id<P, R>>, S, Id<P, R>> = AbstractColumn(name, javaClass<Id<P, R>>(), ColumnType.ID_SET)

fun <S : TableSchema<P>, R: TableSchema<P>, P> nullableId(name: String, schema: R): NullableIdColumn<P, S, R> = NullableIdColumn(name, schema.id.valueClass, ColumnType.FOREIGN_ID)
fun <S : TableSchema<P>, R: TableSchema<P>, P> S.nullableId(name: String, schema: R): NullableIdColumn<P, S, R> = NullableIdColumn(name, schema.id.valueClass, ColumnType.FOREIGN_ID)