package kotlinx.nosql.util

import java.lang.reflect.Field
import java.util.ArrayList
import java.util.HashMap
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.AbstractSchema
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.joda.time.DateTime

fun getAllFields(_type: Class<in Any>, condition: (Field) -> Boolean = { f -> true },
                 fields: MutableList<Field> = ArrayList()): MutableList<Field> {
    for (field in _type.getDeclaredFields()!!) {
        if (condition(field)) fields.add(field)
    }
    if (_type.getSuperclass() != null) {
        getAllFields(_type.getSuperclass()!!, condition, fields)
    }
    return fields
}

fun getAllFieldsMap(_type: Class<in Any>, condition: (Field) -> Boolean = { f -> true },
                    fields: MutableMap<String, Field> = HashMap()): MutableMap<String, Field> {
    for (field in _type.getDeclaredFields()!!) {
        if (condition(field)) fields.put(field.getName()!!.toLowerCase(), field)
    }
    if (_type.getSuperclass() != null) {
        getAllFieldsMap(_type.getSuperclass()!!, condition, fields)
    }
    return fields
}

val Field.isColumn: Boolean
    get() {
        return javaClass<AbstractColumn<Any?, AbstractSchema, Any?>>().isAssignableFrom(this.getType()!!)
    }

fun Field.asColumn(schema: Any): AbstractColumn<*, *, *> {
    this.setAccessible(true)
    return this.get(schema) as AbstractColumn<*, *, *>
}

fun newInstance(clazz: Class<out Any?>): Any {
  val constructor = clazz.getConstructors()!![0]
  val constructorParamTypes = constructor.getParameterTypes()!!
  val constructorParamValues = Array<Any?>(constructor.getParameterTypes()!!.size, { index ->
    when (constructorParamTypes[index].getName()) {
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
