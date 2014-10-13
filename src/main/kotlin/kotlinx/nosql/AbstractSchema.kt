package kotlinx.nosql

import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.joda.time.DateTime
import kotlinx.nosql.query.TextQuery

abstract class AbstractSchema(val schemaName: String) {
    // TODO TODO TODO
    internal val indices = ArrayList<AbstractIndex>()

    // TODO TODO TODO
    // val columns = ArrayList<AbstractColumn<*, *, *>>()

    // TODO TODO TODO
    class object {
        val threadLocale = ThreadLocal<AbstractSchema>()

        fun <T: AbstractSchema> current(): T {
            return threadLocale.get() as T
        }

        fun set(schema: AbstractSchema) {
            return threadLocale.set(schema)
        }
    }
}

// TODO
fun text(search: String): Query {
    return TextQuery(search)
}

// Extension functions

fun <S : AbstractSchema> string(name: String): AbstractColumn<String, S, String> = AbstractColumn(name, javaClass<String>(), ColumnType.STRING)
fun <S : AbstractSchema> S.string(name: String): AbstractColumn<String, S, String> = AbstractColumn(name, javaClass<String>(), ColumnType.STRING)

fun <S : AbstractSchema> boolean(name: String): AbstractColumn<Boolean, S, Boolean> = AbstractColumn(name, javaClass<Boolean>(), ColumnType.BOOLEAN)
fun <S : AbstractSchema> S.boolean(name: String): AbstractColumn<Boolean, S, Boolean> = AbstractColumn(name, javaClass<Boolean>(), ColumnType.BOOLEAN)

fun <S : AbstractSchema> date(name: String): AbstractColumn<LocalDate, S, LocalDate> = AbstractColumn(name, javaClass<LocalDate>(), ColumnType.DATE)
fun <S : AbstractSchema> S.date(name: String): AbstractColumn<LocalDate, S, LocalDate> = AbstractColumn(name, javaClass<LocalDate>(), ColumnType.DATE)

fun <S : AbstractSchema> time(name: String): AbstractColumn<LocalTime, S, LocalTime> = AbstractColumn(name, javaClass<LocalTime>(), ColumnType.TIME)
fun <S : AbstractSchema> S.time(name: String): AbstractColumn<LocalTime, S, LocalTime> = AbstractColumn(name, javaClass<LocalTime>(), ColumnType.TIME)

fun <S : AbstractSchema> dateTime(name: String): AbstractColumn<DateTime, S, DateTime> = AbstractColumn(name, javaClass<DateTime>(), ColumnType.DATE_TIME)
fun <S : AbstractSchema> S.dateTime(name: String): AbstractColumn<DateTime, S, DateTime> = AbstractColumn(name, javaClass<DateTime>(), ColumnType.DATE_TIME)

fun <S : AbstractSchema> double(name: String): AbstractColumn<Double, S, Double> = AbstractColumn(name, javaClass<Double>(), ColumnType.DOUBLE)
fun <S : AbstractSchema> S.double(name: String): AbstractColumn<Double, S, Double> = AbstractColumn(name, javaClass<Double>(), ColumnType.DOUBLE)

fun <S : AbstractSchema> integer(name: String): AbstractColumn<Int, S, Int> = AbstractColumn(name, javaClass<Int>(), ColumnType.INTEGER)
fun <S : AbstractSchema> S.integer(name: String): AbstractColumn<Int, S, Int> = AbstractColumn(name, javaClass<Int>(), ColumnType.INTEGER)

fun <S : AbstractSchema> float(name: String): AbstractColumn<Float, S, Float> = AbstractColumn(name, javaClass<Float>(), ColumnType.FLOAT)
fun <S : AbstractSchema> S.float(name: String): AbstractColumn<Float, S, Float> = AbstractColumn(name, javaClass<Float>(), ColumnType.FLOAT)

fun <S : AbstractSchema> long(name: String): AbstractColumn<Long, S, Long> = AbstractColumn(name, javaClass<Long>(), ColumnType.LONG)
fun <S : AbstractSchema> S.long(name: String): AbstractColumn<Long, S, Long> = AbstractColumn(name, javaClass<Long>(), ColumnType.LONG)

fun <S : AbstractSchema> short(name: String): AbstractColumn<Short, S, Short> = AbstractColumn(name, javaClass<Short>(), ColumnType.SHORT)
fun <S : AbstractSchema> S.short(name: String): AbstractColumn<Short, S, Short> = AbstractColumn(name, javaClass<Short>(), ColumnType.SHORT)

fun <S : AbstractSchema> byte(name: String): AbstractColumn<Byte, S, Byte> = AbstractColumn(name, javaClass<Byte>(), ColumnType.BYTE)
fun <S : AbstractSchema> S.byte(name: String): AbstractColumn<Byte, S, Byte> = AbstractColumn(name, javaClass<Byte>(), ColumnType.BYTE)

fun <S : AbstractSchema> nullableString(name: String): NullableColumn<String, S> = NullableColumn(name, javaClass<String>(), ColumnType.STRING)
fun <S : AbstractSchema> S.nullableString(name: String): NullableColumn<String, S> = NullableColumn(name, javaClass<String>(), ColumnType.STRING)

fun <S : AbstractSchema> nullableInteger(name: String): NullableColumn<Int, S> = NullableColumn(name, javaClass<Int>(), ColumnType.INTEGER)
fun <S : AbstractSchema> S.nullableInteger(name: String): NullableColumn<Int, S> = NullableColumn(name, javaClass<Int>(), ColumnType.INTEGER)

fun <S : AbstractSchema> nullableBoolean(name: String): NullableColumn<Boolean, S> = NullableColumn(name, javaClass<Boolean>(), ColumnType.BOOLEAN)
fun <S : AbstractSchema> S.nullableBoolean(name: String): NullableColumn<Boolean, S> = NullableColumn(name, javaClass<Boolean>(), ColumnType.BOOLEAN)

fun <S : AbstractSchema> nullableDate(name: String): NullableColumn<LocalDate, S> = NullableColumn(name, javaClass<LocalDate>(), ColumnType.DATE)
fun <S : AbstractSchema> S.nullableDate(name: String): NullableColumn<LocalDate, S> = NullableColumn(name, javaClass<LocalDate>(), ColumnType.DATE)

fun <S : AbstractSchema> nullableTime(name: String): NullableColumn<LocalTime, S> = NullableColumn(name, javaClass<LocalTime>(), ColumnType.TIME)
fun <S : AbstractSchema> S.nullableTime(name: String): NullableColumn<LocalTime, S> = NullableColumn(name, javaClass<LocalTime>(), ColumnType.TIME)

fun <S : AbstractSchema> nullableDateTime(name: String): NullableColumn<DateTime, S> = NullableColumn(name, javaClass<DateTime>(), ColumnType.DATE_TIME)
fun <S : AbstractSchema> S.nullableDateTime(name: String): NullableColumn<DateTime, S> = NullableColumn(name, javaClass<DateTime>(), ColumnType.DATE_TIME)

fun <S : AbstractSchema> nullableDouble(name: String): NullableColumn<Double, S> = NullableColumn(name, javaClass<Double>(), ColumnType.DOUBLE)
fun <S : AbstractSchema> S.nullableDouble(name: String): NullableColumn<Double, S> = NullableColumn(name, javaClass<Double>(), ColumnType.DOUBLE)

fun <S : AbstractSchema> nullableFloat(name: String): NullableColumn<Float, S> = NullableColumn(name, javaClass<Float>(), ColumnType.FLOAT)
fun <S : AbstractSchema> S.nullableFloat(name: String): NullableColumn<Float, S> = NullableColumn(name, javaClass<Float>(), ColumnType.FLOAT)

fun <S : AbstractSchema> nullableLong(name: String): NullableColumn<Long, S> = NullableColumn(name, javaClass<Long>(), ColumnType.LONG)
fun <S : AbstractSchema> S.nullableLong(name: String): NullableColumn<Long, S> = NullableColumn(name, javaClass<Long>(), ColumnType.LONG)

fun <S : AbstractSchema> nullableShort(name: String): NullableColumn<Short, S> = NullableColumn(name, javaClass<Short>(), ColumnType.SHORT)
fun <S : AbstractSchema> S.nullableShort(name: String): NullableColumn<Short, S> = NullableColumn(name, javaClass<Short>(), ColumnType.SHORT)

fun <S : AbstractSchema> nullableByte(name: String): NullableColumn<Byte, S> = NullableColumn(name, javaClass<Byte>(), ColumnType.BYTE)
fun <S : AbstractSchema> S.nullableByte(name: String): NullableColumn<Byte, S> = NullableColumn(name, javaClass<Byte>(), ColumnType.BYTE)

fun <S : AbstractSchema> setOfString(name: String): AbstractColumn<Set<String>, S, String> = AbstractColumn<Set<String>, S, String>(name, javaClass(), ColumnType.STRING_SET)
fun <S : AbstractSchema> S.setOfString(name: String): AbstractColumn<Set<String>, S, String> = AbstractColumn<Set<String>, S, String>(name, javaClass<String>(), ColumnType.STRING_SET)

fun <S : AbstractSchema> setOfInteger(name: String): AbstractColumn<Set<Int>, S, Int> = AbstractColumn<Set<Int>, S, Int>(name, javaClass(), ColumnType.INTEGER_SET)
fun <S : AbstractSchema> S.setOfInteger(name: String): AbstractColumn<Set<Int>, S, Int> = AbstractColumn<Set<Int>, S, Int>(name, javaClass<Int>(), ColumnType.INTEGER_SET)

fun <S : AbstractSchema> listOfString(name: String): AbstractColumn<List<String>, S, String> = AbstractColumn<List<String>, S, String>(name, javaClass<String>(), ColumnType.STRING_LIST)
fun <S : AbstractSchema> S.listOfString(name: String): AbstractColumn<List<String>, S, String> = AbstractColumn<List<String>, S, String>(name, javaClass<String>(), ColumnType.STRING_LIST)

fun <S : AbstractSchema> listOfInteger(name: String): AbstractColumn<List<Int>, S, Int> = AbstractColumn<List<Int>, S, Int>(name, javaClass<Int>(), ColumnType.INTEGER_LIST)
fun <S : AbstractSchema> S.listOfInteger(name: String): AbstractColumn<List<Int>, S, Int> = AbstractColumn<List<Int>, S, Int>(name, javaClass<Int>(), ColumnType.INTEGER_LIST)
