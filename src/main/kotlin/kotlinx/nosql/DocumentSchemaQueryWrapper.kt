package kotlinx.nosql

open class DocumentSchemaQueryWrapper<T : DocumentSchema<P, C>, P: Any, C: Any>(val params: kotlinx.nosql.DocumentSchemaQueryParams<T, P, C>): Iterable<C> {
    override fun iterator(): Iterator<C> {
        return Session.current<DocumentSchemaOperations>().find(params)
    }

    fun skip(num: Int): kotlinx.nosql.DocumentSchemaQueryWrapper<T, P, C> {
        params.skip = num
        return this
    }

    fun take(num: Int): DocumentSchemaQueryWrapper<T, P, C> {
        params.take = num
        return this
    }

    fun remove(): Int {
        return Session.current<Session>().delete(params.schema, params.query!!)
    }

    fun <X> projection(x: T.() -> X): X {
        val xx = params.schema.x()
        val projectionParams = kotlinx.nosql.TableSchemaProjectionQueryParams<TableSchema<Any>, Any, Any>(params.schema as TableSchema<Any>,
                when (xx) {
                    is AbstractColumn<*, *, *> -> listOf(xx)
                    is ColumnPair<*, *, *> -> listOf(xx.a, xx.b)
                    is ColumnTriple<*, *, *, *> -> listOf(xx.a, xx.b, xx.c)
                    is kotlinx.nosql.ColumnQuadruple<*, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d)
                    is ColumnQuintuple<*, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e)
                    is ColumnSextuple<*, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f)
                    is ColumnSeptuple<*, *, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f, xx.g)
                    is ColumnOctuple<*, *, *, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f, xx.g, xx.h)
                    is ColumnNonuple<*, *, *, *, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f, xx.g, xx.h, xx.j)
                    is kotlinx.nosql.ColumnDecuple<*, *, *, *, *, *, *, *, *, *, *> -> listOf(xx.a, xx.b, xx.c, xx.d, xx.e, xx.f, xx.g, xx.h, xx.i, xx.j)
                    else -> throw UnsupportedOperationException()
                }, params.query)
        TableSchemaProjectionQueryWrapper.set(TableSchemaProjectionQueryWrapper(projectionParams))
        return params.schema.x()
    }
}