package kotlinx.nosql.util

import java.lang.reflect.Field
import java.util.ArrayList
import java.util.HashMap
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.Schema

fun getAllFields(_type: Class<in Any>, condition: (Field) -> Boolean = { f -> true },
                 fields: MutableList<Field> = ArrayList()): MutableList<Field> {
    for (field in _type.getDeclaredFields()) {
        if (condition(field)) fields.add(field)
    }
    if (_type.getSuperclass() != null) {
        getAllFields(_type.getSuperclass()!!, condition, fields);
    }
    return fields
}

fun getAllFieldsMap(_type: Class<in Any>, condition: (Field) -> Boolean = { f -> true },
                    fields: MutableMap<String, Field> = HashMap()): MutableMap<String, Field> {
    for (field in _type.getDeclaredFields()) {
        if (condition(field)) fields.put(field.getName()!!.toLowerCase(), field)
    }
    if (_type.getSuperclass() != null) {
        getAllFieldsMap(_type.getSuperclass()!!, condition, fields);
    }
    return fields
}

val Field.isColumn: Boolean
    get() {
        return javaClass<AbstractColumn<Any?, Schema, Any?>>().isAssignableFrom(this.getType()!!)
    }

fun Field.asColumn(schema: Any): AbstractColumn<*, *, *> {
    this.setAccessible(true)
    return this.get(schema) as AbstractColumn<*, *, *>
}