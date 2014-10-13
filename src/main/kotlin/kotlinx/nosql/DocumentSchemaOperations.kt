package kotlinx.nosql

import kotlinx.nosql.query.NoQuery

trait DocumentSchemaOperations {
    fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T>
    fun <T: DocumentSchema<P, C>, P, C> T.find(query: T.() -> Query = { NoQuery }): DocumentSchemaQueryWrapper<T, P, C>
    // TODO: Implement find(id) -> Wrapper
    /*fun <T: DocumentSchema<P, C>, P, C> T.find(id: Id<P, T>): C? {
        val w = find { this.id.equal(id) }
        return if (w.count() > 0) w.single() else null
    }*/

    internal fun <T : DocumentSchema<P, C>, P, C> find(params: DocumentSchemaQueryParams<T, P, C>): Iterator<C>
}
