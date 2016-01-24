package kotlinx.nosql.util

import java.lang.reflect.Field
import java.util.ArrayList
import java.util.HashMap
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.AbstractSchema
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.joda.time.DateTime
import kotlin.collections.listOf
import kotlin.collections.setOf
import kotlin.text.toLowerCase

fun getAllFields(_type: Class<in Any>, condition: (Field) -> Boolean = { f -> true },
                 fields: MutableList<Field> = ArrayList()): MutableList<Field> {
    for (field in _type.declaredFields!!) {
        if (condition(field)) fields.add(field)
    }
    if (_type.superclass != null) {
        getAllFields(_type.superclass!!, condition, fields)
    }
    return fields
}

fun getAllFieldsMap(_type: Class<in Any>, condition: (Field) -> Boolean = { f -> true },
                    fields: MutableMap<String, Field> = HashMap()): MutableMap<String, Field> {
    for (field in _type.declaredFields!!) {
        if (condition(field)) fields.put(field.name!!.toLowerCase(), field)
    }
    if (_type.superclass != null) {
        getAllFieldsMap(_type.superclass, condition, fields)
    }
    return fields
}

val Field.isColumn: Boolean
    get() {
        return AbstractColumn::class.java.isAssignableFrom(this.type!!)
    }

fun Field.asColumn(schema: Any): AbstractColumn<*, *, *> {
    this.isAccessible = true
    return this.get(schema) as AbstractColumn<*, *, *>
}

fun newInstance(clazz: Class<out Any?>): Any {
  val constructor = clazz.constructors!![0]
  val constructorParamTypes = constructor.parameterTypes!!
  val constructorParamValues = Array<Any?>(constructor.parameterTypes!!.size(), { index: Int ->
    when (constructorParamTypes[index].name) {
      "int" -> 0
      "java.lang.String" -> ""
      "org.joda.time.LocalDate" -> LocalDate()
      "org.joda.time.LocalTime" -> LocalTime()
      "org.joda.time.DateTime" -> DateTime()
      "double" -> 0.toDouble()
      "float" -> 0.toFloat()
      "long" -> 0.toLong()
      "short" -> 0.toShort()
      "byte" -> 0.toByte()
      "boolean" -> false
      "java.util.List" -> listOf<Any>()
      "java.util.Set" -> setOf<Any>()
      else -> newInstance(constructorParamTypes[index])
    }
  })
  return constructor.newInstance(*constructorParamValues)!!
}
