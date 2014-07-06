package kotlinx.nosql

import java.util.ArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.joda.time.DateTime
import rx.Observable
import rx.Observable.OnSubscribeFunc
import rx.subscriptions.Subscriptions

abstract class AbstractSchema(val schemaName: String) {
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

abstract class KeyValueSchema(name: String): AbstractSchema(name) {
}

abstract class AbstractTableSchema(name: String): AbstractSchema(name) {
}

class Id<I, R: TableSchema<I>>(val value: I) {
    override fun toString() = value.toString()

    override fun equals(other: Any?): Boolean {
        return if (other is Id<*, *>) value.equals((other as Id<*, *>).value) else false
    }
    override fun hashCode(): Int {
        return value.hashCode()
    }
}

abstract class TableSchema<I>(tableName: String, primaryKey: AbstractColumn<I, out TableSchema<I>, I>): AbstractTableSchema(tableName) {
    val pk = AbstractColumn<Id<I, TableSchema<I>>, TableSchema<I>, I>(primaryKey.name, primaryKey.valueClass, ColumnType.PRIMARY_ID)
}

open class PrimaryKey<I>(val name: String, val javaClass: Class<I>, val columnType: ColumnType) {
    class object {
        fun string(name: String) = PrimaryKey<String>(name, javaClass<String>(), ColumnType.STRING)
        fun integer(name: String) = PrimaryKey<Int>(name, javaClass<Int>(), ColumnType.INTEGER)
    }
}

val <C, T : TableSchema<C>> T.id: AbstractColumn<Id<C, T>, T, C>
    get () {
        return pk as AbstractColumn<Id<C, T>, T, C>
    }

class Discriminator<D, S : DocumentSchema<out Any, out Any>>(val column: AbstractColumn<D, S, D>, val value: D) {
}

abstract class DocumentSchema<I, out D>(name: String, val valueClass: Class<D>, primaryKey: AbstractColumn<I,
        out DocumentSchema<I, D>, I>, val discriminator: Discriminator<out Any, out DocumentSchema<I, D>>? = null) : TableSchema<I>(name, primaryKey) {
    {
        if (discriminator != null) {
            val emptyDiscriminators = CopyOnWriteArrayList<Discriminator<*, *>>()
            val discriminators = tableDiscriminators.putIfAbsent(name, emptyDiscriminators)
            if (discriminators != null)
                discriminators.add(discriminator)
            else
                emptyDiscriminators.add(discriminator)
            // TODO TODO TODO
            discriminatorClasses.put(discriminator, this.valueClass)
            discriminatorSchemaClasses.put(discriminator, this.javaClass)
            discriminatorSchemas.put(discriminator, this)
        }
    }

    class object {
        val tableDiscriminators = ConcurrentHashMap<String, MutableList<Discriminator<*, *>>>()
        val discriminatorClasses = ConcurrentHashMap<Discriminator<*, *>, Class<*>>()
        val discriminatorSchemaClasses = ConcurrentHashMap<Discriminator<*, *>, Class<*>>()
        val discriminatorSchemas = ConcurrentHashMap<Discriminator<*, *>, AbstractSchema>()
    }
}

/*
abstract class AbstractDocument<I, S: DocumentSchema<I, AbstractDocument<I, S>>> {
    val id: Id<I, S>? = null
}
*/

fun <S : TableSchema<P>, R: TableSchema<P>, P> id(name: String, schema: R): AbstractColumn<Id<P, R>, S, P> = AbstractColumn(name, schema.id.valueClass, ColumnType.FOREIGN_ID)
fun <S : TableSchema<P>, R: TableSchema<P>, P> S.id(name: String, schema: R): AbstractColumn<Id<P, R>, S, P> = AbstractColumn(name, schema.id.valueClass, ColumnType.FOREIGN_ID)

fun <S : TableSchema<P>, R: TableSchema<P>, P>  setOfId(name: String, schema: R): AbstractColumn<Set<Id<P, R>>, S, Id<P, R>> = AbstractColumn(name, javaClass<Id<P, R>>(), ColumnType.ID_SET)
fun <S : TableSchema<P>, R: TableSchema<P>, P> S.setOfId(name: String, schema: R): AbstractColumn<Set<Id<P, R>>, S, Id<P, R>> = AbstractColumn(name, javaClass<Id<P, R>>(), ColumnType.ID_SET)

fun <S : TableSchema<P>, R: TableSchema<P>, P> nullableId(name: String, schema: R): NullableIdColumn<P, S, R> = NullableIdColumn(name, schema.id.valueClass, ColumnType.FOREIGN_ID)
fun <S : TableSchema<P>, R: TableSchema<P>, P> S.nullableId(name: String, schema: R): NullableIdColumn<P, S, R> = NullableIdColumn(name, schema.id.valueClass, ColumnType.FOREIGN_ID)

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

//fun <T: AbstractSchema, C> T.setColumn(name: String, javaClass: Class<C>): SetColumn<C, T> = SetColumn(name, javaClass)

fun <S : AbstractSchema> setOfString(name: String): AbstractColumn<Set<String>, S, String> = AbstractColumn<Set<String>, S, String>(name, javaClass(), ColumnType.STRING_SET)

fun <S : AbstractSchema> S.setOfString(name: String): AbstractColumn<Set<String>, S, String> = AbstractColumn<Set<String>, S, String>(name, javaClass<String>(), ColumnType.STRING_SET)

fun <S : AbstractSchema> S.setOfInteger(name: String): AbstractColumn<Set<Int>, S, Int> = AbstractColumn<Set<Int>, S, Int>(name, javaClass<Int>(), ColumnType.INTEGER_SET)

//fun <T: AbstractSchema, C> T.listColumn(name: String, javaClass: Class<C>): ListColumn<C, T> = ListColumn(name, javaClass)

fun <S : AbstractSchema> listOfString(name: String): AbstractColumn<List<String>, S, String> = AbstractColumn<List<String>, S, String>(name, javaClass<String>(), ColumnType.STRING_LIST)

fun <S : AbstractSchema> S.listOfString(name: String): AbstractColumn<List<String>, S, String> = AbstractColumn<List<String>, S, String>(name, javaClass<String>(), ColumnType.STRING_LIST)

fun <S : AbstractSchema> S.listOfInteger(name: String): AbstractColumn<List<Int>, S, Int> = AbstractColumn<List<Int>, S, Int>(name, javaClass<Int>(), ColumnType.INTEGER_LIST)

/*
// TODO TODO TODO
fun <S : AbstractTableSchema, X> S.select(selector: S.() -> X): X {
    AbstractSchema.set(this)
    return selector();
}
*/

fun <S : AbstractTableSchema, B> FilterQuery<S>.map(statement: S.(Map<Any, Any>) -> B): List<B> {
    val results = ArrayList<B>()
    //Query
    return results
}

class Template1<S : AbstractTableSchema, A>(val table: S, val a: AbstractColumn<A, S, *>) {
    fun invoke(av: A): Array<Pair<AbstractColumn<*, S, *>, *>> {
        return array(Pair(a, av))
    }
}

class Quadruple<A1, A2, A3, A4>(val a1: A1, val a2: A2, val a3: A3, val a4: A4) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
}

class Quintuple<A1, A2, A3, A4, A5>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
    public fun component5(): A5 = a5
}

class Sextuple<A1, A2, A3, A4, A5, A6>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5, val a6: A6) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
    public fun component5(): A5 = a5
    public fun component6(): A6 = a6
}

class Septuple<A1, A2, A3, A4, A5, A6, A7>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5, val a6: A6, val a7: A7) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
    public fun component5(): A5 = a5
    public fun component6(): A6 = a6
    public fun component7(): A7 = a7
}

class Octuple<A1, A2, A3, A4, A5, A6, A7, A8>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5, val a6: A6, val a7: A7, val a8: A8) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
    public fun component5(): A5 = a5
    public fun component6(): A6 = a6
    public fun component7(): A7 = a7
    public fun component8(): A8 = a8
}

class Nonuple<A1, A2, A3, A4, A5, A6, A7, A8, A9>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5, val a6: A6, val a7: A7, val a8: A8, val a9: A9) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
    public fun component5(): A5 = a5
    public fun component6(): A6 = a6
    public fun component7(): A7 = a7
    public fun component8(): A8 = a8
    public fun component9(): A9 = a9
}

class Decuple<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5, val a6: A6, val a7: A7, val a8: A8, val a9: A9, val a10: A10) {
    public fun component1(): A1 = a1
    public fun component2(): A2 = a2
    public fun component3(): A3 = a3
    public fun component4(): A4 = a4
    public fun component5(): A5 = a5
    public fun component6(): A6 = a6
    public fun component7(): A7 = a7
    public fun component8(): A8 = a8
    public fun component9(): A9 = a9
    public fun component10(): A10 = a10
}

fun <S : AbstractTableSchema, A, B> S.template(a: AbstractColumn<A, S, *>, b: AbstractColumn<B, S, *>): Template2<S, A, B> {
    return Template2(a, b)
}

class Template2<S : AbstractSchema, A, B>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>): ColumnObservable<Pair<A, B>>() {
    fun <C> plus(c: AbstractColumn<C, S, *>): Template3<S, A, B, C> {
        return Template3(a, b, c)
    }
}

class Template3<S : AbstractSchema, A, B, C>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>, val c: AbstractColumn<C, S, *>): ColumnObservable<Triple<A, B, C>>() {
    fun <D> plus(d: AbstractColumn<D, S, *>): Template4<S, A, B, C, D> {
        return Template4(a, b, c, d)
    }
}

/*fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.insert(statement: () -> Triple<P, A, B>) {
    val tt = statement()
    Session.current().insert(array(Pair(Schema.current<T>().ID, tt.component1()), Pair(a, tt.component2()), Pair(b, tt.component3())))
}

fun <T : TableSchema<P>, P, C> AbstractColumn<C, T, *>.insert(statement: () -> Pair<P, C>) {
    val tt = statement()
    val id: AbstractColumn<P, T, *> = Schema.current<T>().ID // Type inference failure
    Session.current().insert(array(Pair(id, tt.component1()), Pair(this, tt.component2())))
}*/

class Template4<S : AbstractSchema, A, B, C, D>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>, val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>): ColumnObservable<Quadruple<A, B, C, D>>() {
    fun <E> plus(e: AbstractColumn<E, S, *>): Template5<S, A, B, C, D, E> {
        return Template5(a, b, c, d, e)
    }

    fun insert(statement: () -> Quadruple<A, B, C, D>) {
        val tt = statement()
        Session.current().insert(array(Pair(a, tt.component1()), Pair(b, tt.component2()), Pair(c, tt.component3()), Pair(d, tt.component4())))
    }
}

class Template5<S : AbstractSchema, A, B, C, D, E>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                          val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                          val e: AbstractColumn<E, S, *>): ColumnObservable<Quintuple<A, B, C, D, E>>() {
    fun <F> plus(f: AbstractColumn<F, S, *>): Template6<S, A, B, C, D, E, F> {
        return Template6(a, b, c, d, e, f)
    }
}

class Template6<S : AbstractSchema, A, B, C, D, E, F>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                          val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                          val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>): ColumnObservable<Sextuple<A, B, C, D, E, F>>() {
    fun <G> plus(g: AbstractColumn<G, S, *>): Template7<S, A, B, C, D, E, F, G> {
        return Template7(a, b, c, d, e, f, g)
    }
}

class Template7<S : AbstractSchema, A, B, C, D, E, F, G>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                             val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                             val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>,
                                             val g: AbstractColumn<G, S, *>): ColumnObservable<Septuple<A, B, C, D, E, F, G>>() {
    fun <H> plus(h: AbstractColumn<H, S, *>): Template8<S, A, B, C, D, E, F, G, H> {
        return Template8(a, b, c, d, e, f, g, h)
    }
}

class Template8<S : AbstractSchema, A, B, C, D, E, F, G, H>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>,
                                                val g: AbstractColumn<G, S, *>, val h: AbstractColumn<H, S, *>): ColumnObservable<Octuple<A, B, C, D, E, F, G, H>>() {
    fun <J> plus(j: AbstractColumn<J, S, *>): Template9<S, A, B, C, D, E, F, G, H, J> {
        return Template9(a, b, c, d, e, f, g, h, j)
    }
}

class Template9<S : AbstractSchema, A, B, C, D, E, F, G, H, J>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                   val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                   val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>,
                                                   val g: AbstractColumn<G, S, *>, val h: AbstractColumn<H, S, *>,
                                                   val j: AbstractColumn<J, S, *>): ColumnObservable<Nonuple<A, B, C, D, E, F, G, H, J>>() {
    fun <K> plus(k: AbstractColumn<K, S, *>): Template10<S, A, B, C, D, E, F, G, H, J, K> {
        return Template10(a, b, c, d, e, f, g, h, j, k)
    }
}

class Template10<S : AbstractSchema, A, B, C, D, E, F, G, H, J, K>(val a: AbstractColumn<A, S, *>, val b: AbstractColumn<B, S, *>,
                                                      val c: AbstractColumn<C, S, *>, val d: AbstractColumn<D, S, *>,
                                                      val e: AbstractColumn<E, S, *>, val f: AbstractColumn<F, S, *>,
                                                      val g: AbstractColumn<G, S, *>, val h: AbstractColumn<H, S, *>,
                                                      val i: AbstractColumn<J, S, *>, val j: AbstractColumn<K, S, *>): ColumnObservable<Decuple<A, B, C, D, E, F, G, H, J, K>>() {
}