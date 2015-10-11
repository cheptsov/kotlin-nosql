package kotlinx.nosql

abstract class TableSchema<I: Any>(tableName: String, primaryKey: AbstractColumn<I, out TableSchema<I>, I>): AbstractTableSchema(tableName) {
    val pk = AbstractColumn<Id<I, TableSchema<I>>, TableSchema<I>, I>(primaryKey.name, primaryKey.valueClass, ColumnType.PRIMARY_ID)
}

// Extension functions

val <C: Any, T : TableSchema<C>> T.id: AbstractColumn<Id<C, T>, T, C>
    get () {
        return pk as AbstractColumn<Id<C, T>, T, C>
    }

fun <S : AbstractSchema, R: TableSchema<P>, P: Any> id(name: String, refSchema: R): AbstractColumn<Id<P, R>, S, P> = AbstractColumn(name, refSchema.id.valueClass, ColumnType.FOREIGN_ID)
fun <S : AbstractSchema, R: TableSchema<P>, P: Any> S.id(name: String, refSchema: R): AbstractColumn<Id<P, R>, S, P> = AbstractColumn(name, refSchema.id.valueClass, ColumnType.FOREIGN_ID)

fun <S : TableSchema<P>, R: TableSchema<P>, P: Any>  listOfId(name: String, refSchema: R): IdListColumn<S, R, P> = IdListColumn(name, refSchema)
fun <S : TableSchema<P>, R: TableSchema<P>, P: Any> S.listOfId(name: String, refSchema: R): IdListColumn<S, R, P> = IdListColumn(name, refSchema)

fun <S : TableSchema<P>, R: TableSchema<P>, P: Any>  setOfId(name: String, refSchema: R): IdSetColumn<S, R, P> = IdSetColumn(name, refSchema)
fun <S : TableSchema<P>, R: TableSchema<P>, P: Any> S.setOfId(name: String, refSchema: R): IdSetColumn<S, R, P> = IdSetColumn(name, refSchema)

fun <S : TableSchema<P>, R: TableSchema<P>, P: Any> nullableId(name: String, refSchema: R): NullableIdColumn<P, S, R> = NullableIdColumn(name, refSchema.id.valueClass, ColumnType.FOREIGN_ID)
fun <S : TableSchema<P>, R: TableSchema<P>, P: Any> S.nullableId(name: String, refSchema: R): NullableIdColumn<P, S, R> = NullableIdColumn(name, refSchema.id.valueClass, ColumnType.FOREIGN_ID)
