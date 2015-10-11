package kotlinx.nosql

import kotlinx.nosql.query.NoQuery

interface KeyValueDocumentSchemaOperations {
  fun <T : DocumentSchema<P, V>, P: Any, V: Any> T.insert(v: V): Id<P, T>
  operator fun <T: DocumentSchema<P, C>, P: Any, C: Any> T.get(id: Id<P, T>): C?
  fun <T: DocumentSchema<P, C>, P: Any, C: Any> T.find(id: Id<P, T>): DocumentSchemaIdQueryWrapper<T, P, C> {
      return DocumentSchemaIdQueryWrapper(this, id)
  }
}
