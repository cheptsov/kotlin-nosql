package kotlin.nosql

import java.util.ArrayList
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

abstract class AbstractSchema(val name: String) {
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

abstract class TableSchema(name: String): AbstractSchema(name) {
    val primaryKeys = ArrayList<PKColumn<*, *>>()
}

abstract class PKTableSchema<P>(tableName: String, primaryKey: PK<P>): TableSchema(tableName) {
    val pk = PKColumn<P, PKTableSchema<P>>(this, primaryKey.name, primaryKey.javaClass, primaryKey.columnType)
}

open class PK<P>(val name: jet.String, val javaClass: Class<P>, val columnType: ColumnType) {
    class object {
        fun string(name: jet.String) = PK<jet.String>(name, javaClass<jet.String>(), ColumnType.STRING)
        fun integer(name: jet.String) = PK<Int>(name, javaClass<Int>(), ColumnType.INTEGER)
    }
}

val <C, T : PKTableSchema<C>> T.ID: AbstractColumn<C, T, C>
    get () {
        return pk as AbstractColumn<C, T, C>
    }

class Discriminator<V, T: DocumentSchema<out Any, out Any>>(val column: AbstractColumn<V, T, V>, val value: V) {
}

abstract class DocumentSchema<P, V>(name: String, val valueClass: Class<V>, primaryKey: AbstractColumn<P,
        out DocumentSchema<P, V>, P>) : PKTableSchema<P>(name, PK<P>(primaryKey.name, primaryKey.valueClass, primaryKey.columnType)) {
}

abstract class PolymorphicSchema<P, V>(name: String, valueClass: Class<V>, primaryKey: AbstractColumn<P,
        out DocumentSchema<P, V>, P>, val discriminator: Discriminator<out Any, out DocumentSchema<P, V>>) : DocumentSchema<P, V>(name, valueClass, primaryKey) {
    {
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

    class object {
        val tableDiscriminators = ConcurrentHashMap<String, MutableList<Discriminator<*, *>>>()
        val discriminatorClasses = ConcurrentHashMap<Discriminator<*, *>, Class<*>>()
        val discriminatorSchemaClasses = ConcurrentHashMap<Discriminator<*, *>, Class<*>>()
        val discriminatorSchemas = ConcurrentHashMap<Discriminator<*, *>, AbstractSchema>()
    }
}

fun <T: AbstractSchema> string(name: String): AbstractColumn<String, T, String> = AbstractColumn(name, javaClass<String>(), ColumnType.STRING)

fun <T: AbstractSchema> T.string(name: String): AbstractColumn<String, T, String> = AbstractColumn(name, javaClass<String>(), ColumnType.STRING)

fun <T: AbstractSchema> integer(name: String): AbstractColumn<Int, T, Int> = AbstractColumn(name, javaClass<Int>(), ColumnType.INTEGER)
fun <T: AbstractSchema> T.integer(name: String): AbstractColumn<Int, T, Int> = AbstractColumn(name, javaClass<Int>(), ColumnType.INTEGER)

fun <T: AbstractSchema> T.nullableString(name: String): NullableColumn<String, T> = NullableColumn(name, javaClass<String>(), ColumnType.STRING)

fun <T: AbstractSchema> T.nullableInteger(name: String): NullableColumn<Int, T> = NullableColumn(name, javaClass<Int>(), ColumnType.INTEGER)

//fun <T: AbstractSchema, C> T.setColumn(name: String, javaClass: Class<C>): SetColumn<C, T> = SetColumn(name, javaClass)

fun <T: AbstractSchema> setOfString(name: String): SetColumn<String, T> = SetColumn(name, javaClass<String>(), ColumnType.STRING_SET)

fun <T: AbstractSchema> T.setOfString(name: String): SetColumn<String, T> = SetColumn(name, javaClass<String>(), ColumnType.STRING_SET)

fun <T: AbstractSchema> T.setOfInteger(name: String): SetColumn<Int, T> = SetColumn(name, javaClass<Int>(), ColumnType.INTEGER_SET)

//fun <T: AbstractSchema, C> T.listColumn(name: String, javaClass: Class<C>): ListColumn<C, T> = ListColumn(name, javaClass)

fun <T: AbstractSchema> listOfString(name: String): ListColumn<String, T> = ListColumn(name, javaClass<String>(), ColumnType.STRING_LIST)

fun <T: AbstractSchema> T.listOfString(name: String): ListColumn<String, T> = ListColumn(name, javaClass<String>(), ColumnType.STRING_LIST)

fun <T: AbstractSchema> T.listOfInteger(name: String): ListColumn<Int, T> = ListColumn(name, javaClass<Int>(), ColumnType.INTEGER_LIST)

fun <T: TableSchema> T.delete(body: T.() -> Op) {
    FilterQuery(this, body()) delete { }
}

// TODO TODO TODO
fun <T: TableSchema, X> T.columns(selector: T.() -> X): X {
    AbstractSchema.set(this)
    return selector();
}

fun <T: TableSchema, B> FilterQuery<T>.map(statement: T.(Map<Any, Any>) -> B): List<B> {
    val results = ArrayList<B>()
    //Query
    return results
}

class Template1<T: TableSchema, A>(val table: T, val a: AbstractColumn<A, T, *>) {
    fun invoke(av: A): Array<Pair<AbstractColumn<*, T, *>, *>> {
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

fun <T: TableSchema, A, B> T.template(a: AbstractColumn<A, T, *>, b: AbstractColumn<B, T, *>): Template2<T, A, B> {
    return Template2(a, b)
}

class Template2<T: AbstractSchema, A, B>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>) {
    /*fun invoke(av: A, bv: B): Array<Pair<Column<*, T>, *>> {
        return array(Pair(a, av), Pair(b, bv))
    }*/

    fun <C> plus(c: AbstractColumn<C, T, *>): Template3<T, A, B, C> {
        return Template3(a, b, c)
    }

    fun put(statement: () -> Pair<A, B>) {
        val tt = statement()
        Session.current().insert(array(Pair(a, tt.first), Pair(b, tt.second)))
    }

    /*fun values(va: A, vb: B) {
        Session.get().insert(array(Pair(a, va), Pair(b, vb)))
    }*/
}

class Template3<T: AbstractSchema, A, B, C>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>, val c: AbstractColumn<C, T, *>) {
    fun invoke(av: A, bv: B, cv: C): Array<Pair<AbstractColumn<*, T, *>, *>> {
        return array(Pair(a, av), Pair(b, bv), Pair(c, cv))
    }

    /*fun invoke(): List<Quad<A, B, C, D>> {
        val results = ArrayList<Quad<A, B, C, D>>()
        Query<Quad<A, B, C, D>>(Session.get(), array(a, b, c, d)).forEach{ results.add(it) }
        return results
    }*/

    fun values(va: A, vb: B, vc: C) {
        Session.current().insert(array(Pair(a, va), Pair(b, vb), Pair(c, vc)))
    }

    fun put(statement: () -> Triple<A, B, C>) {
        val tt = statement()
        Session.current().insert(array(Pair(a, tt.component1()), Pair(b, tt.component2()), Pair(c, tt.component3())))
    }

    fun <D> plus(d: AbstractColumn<D, T, *>): Template4<T, A, B, C, D> {
        return Template4(a, b, c, d)
    }
}

fun <T : PKTableSchema<P>, P, A, B> Template2<T, A, B>.insert(statement: () -> Triple<P, A, B>) {
    val tt = statement()
    Session.current().insert(array(Pair(AbstractSchema.current<T>().ID, tt.component1()), Pair(a, tt.component2()), Pair(b, tt.component3())))
}

fun <T : PKTableSchema<P>, P, C> AbstractColumn<C, T, *>.insert(statement: () -> Pair<P, C>) {
    val tt = statement()
    val id: AbstractColumn<P, T, *> = AbstractSchema.current<T>().ID // Type inference failure
    Session.current().insert(array(Pair(id, tt.component1()), Pair(this, tt.component2())))
}

class Template4<T: AbstractSchema, A, B, C, D>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>, val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>) {
    fun invoke(av: A, bv: B, cv: C, dv: D): Array<Pair<AbstractColumn<*, T, *>, *>> {
        return array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv))
    }

    fun insert(statement: () -> Quadruple<A, B, C, D>) {
        val tt = statement()
        Session.current().insert(array(Pair(a, tt.component1()), Pair(b, tt.component2()), Pair(c, tt.component3()), Pair(d, tt.component4())))
    }
}