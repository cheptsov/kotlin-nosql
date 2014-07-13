package kotlinx.nosql

class DocumentSchemaQueryParams<T : DocumentSchema<P, C>, P, C>(val schema: T, val query: Query? = null,
                                                                var skip: Int? = null, var take: Int? = null, var subscribed: Boolean = false)
