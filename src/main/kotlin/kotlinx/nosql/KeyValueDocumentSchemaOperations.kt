package kotlinx.nosql

import kotlinx.nosql.query.NoQuery

trait KeyValueDocumentSchemaOperations {
  fun <T : DocumentSchema<P, V>, P, V> T.insert(v: V): Id<P, T>
  internal fun <T: DocumentSchema<P, C>, P, C> T.get(id: Id<P, T>): C?
  fun <T: DocumentSchema<P, C>, P, C> T.find(id: Id<P, T>): DocumentSchemaIdQueryWrapper<T, P, C> {
      return DocumentSchemaIdQueryWrapper(this, id)
  }
}
