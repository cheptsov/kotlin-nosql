package kotlinx.nosql

import java.util.ArrayList

class DocumentSchemaIdQueryWrapper<T : DocumentSchema<P, C>, P, C>(val schema: T, val id: Id<P, T>): DocumentSchemaQueryWrapper<T, P, C>(DocumentSchemaQueryParams(schema,
        schema.id.equal(id))
) {
    deprecated("Use single()")
    fun get(): C {
        return single()
    }

    override fun iterator(): Iterator<C> {
        val list = ArrayList<C>()
        val value = with (Session.current<KeyValueDocumentSchemaOperations>()) { schema.get(id) }
        if (value != null) {
            list.add(value)
        }
        return list.iterator()
    }
}