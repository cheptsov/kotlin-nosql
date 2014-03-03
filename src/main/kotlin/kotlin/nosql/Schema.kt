package kotlin.nosql

import java.util.ArrayList
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

abstract class Schema(val name: String) {
    // TODO TODO TODO
    // val columns = ArrayList<AbstractColumn<*, *, *>>()

    // TODO TODO TODO
    class object {
        val threadLocale = ThreadLocal<Schema>()

        fun <T: Schema> current(): T {
            return threadLocale.get() as T
        }

        fun set(schema: Schema) {
            return threadLocale.set(schema)
        }
    }
}

abstract class KeyValueSchema(name: String): Schema(name) {
}

abstract class AbstractTableSchema(name: String): Schema(name) {
}

abstract class TableSchema<P>(tableName: String, primaryKey: AbstractColumn<P, out TableSchema<P>, P>): AbstractTableSchema(tableName) {
    val pk = PrimaryKeyColumn<P, TableSchema<P>>(this, primaryKey.name, primaryKey.valueClass, primaryKey.columnType)
}

open class PrimaryKey<P>(val name: jet.String, val javaClass: Class<P>, val columnType: ColumnType) {
    class object {
        fun string(name: jet.String) = PrimaryKey<jet.String>(name, javaClass<jet.String>(), ColumnType.STRING)
        fun integer(name: jet.String) = PrimaryKey<Int>(name, javaClass<Int>(), ColumnType.INTEGER)
    }
}

val <C, T : TableSchema<C>> T.ID: AbstractColumn<C, T, C>
    get () {
        return pk as AbstractColumn<C, T, C>
    }

class Discriminator<V, T: DocumentSchema<out Any, out Any>>(val column: AbstractColumn<V, T, V>, val value: V) {
}

abstract class DocumentSchema<P, V>(name: String, val valueClass: Class<V>, primaryKey: AbstractColumn<P,
        out DocumentSchema<P, V>, P>) : TableSchema<P>(name, primaryKey) {
}

// TODO TODO TODO Join DocumentSchema
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
        val discriminatorSchemas = ConcurrentHashMap<Discriminator<*, *>, Schema>()
    }
}

fun <T: Schema> string(name: String): AbstractColumn<String, T, String> = AbstractColumn(name, javaClass<String>(), ColumnType.STRING)

fun <T: Schema> T.string(name: String): AbstractColumn<String, T, String> = AbstractColumn(name, javaClass<String>(), ColumnType.STRING)

fun <T: Schema> integer(name: String): AbstractColumn<Int, T, Int> = AbstractColumn(name, javaClass<Int>(), ColumnType.INTEGER)
fun <T: Schema> T.integer(name: String): AbstractColumn<Int, T, Int> = AbstractColumn(name, javaClass<Int>(), ColumnType.INTEGER)

fun <T: Schema> T.nullableString(name: String): NullableColumn<String, T> = NullableColumn(name, javaClass<String>(), ColumnType.STRING)

fun <T: Schema> T.nullableInteger(name: String): NullableColumn<Int, T> = NullableColumn(name, javaClass<Int>(), ColumnType.INTEGER)

//fun <T: AbstractSchema, C> T.setColumn(name: String, javaClass: Class<C>): SetColumn<C, T> = SetColumn(name, javaClass)

fun <T: Schema> setOfString(name: String): AbstractColumn<Set<String>, T, String> = AbstractColumn<Set<String>, T, String>(name, javaClass(), ColumnType.STRING_SET)

fun <T: Schema> T.setOfString(name: String): AbstractColumn<Set<String>, T, String> = AbstractColumn<Set<String>, T, String>(name, javaClass<String>(), ColumnType.STRING_SET)

fun <T: Schema> T.setOfInteger(name: String): AbstractColumn<Set<Int>, T, Int> = AbstractColumn<Set<Int>, T, Int>(name, javaClass<Int>(), ColumnType.INTEGER_SET)

//fun <T: AbstractSchema, C> T.listColumn(name: String, javaClass: Class<C>): ListColumn<C, T> = ListColumn(name, javaClass)

fun <T: Schema> listOfString(name: String): AbstractColumn<List<String>, T, String> = AbstractColumn<List<String>, T, String>(name, javaClass<String>(), ColumnType.STRING_LIST)

fun <T: Schema> T.listOfString(name: String): AbstractColumn<List<String>, T, String> = AbstractColumn<List<String>, T, String>(name, javaClass<String>(), ColumnType.STRING_LIST)

fun <T: Schema> T.listOfInteger(name: String): AbstractColumn<List<Int>, T, Int> = AbstractColumn<List<Int>, T, Int>(name, javaClass<Int>(), ColumnType.INTEGER_LIST)

fun <T: AbstractTableSchema> T.delete(body: T.() -> Op) {
    FilterQuery(this, body()) delete { }
}

// TODO TODO TODO
fun <T: AbstractTableSchema, X> T.columns(selector: T.() -> X): X {
    Schema.set(this)
    return selector();
}

fun <T: AbstractTableSchema, B> FilterQuery<T>.map(statement: T.(Map<Any, Any>) -> B): List<B> {
    val results = ArrayList<B>()
    //Query
    return results
}

class Template1<T: AbstractTableSchema, A>(val table: T, val a: AbstractColumn<A, T, *>) {
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

fun <T: AbstractTableSchema, A, B> T.template(a: AbstractColumn<A, T, *>, b: AbstractColumn<B, T, *>): Template2<T, A, B> {
    return Template2(a, b)
}

class Template2<T: Schema, A, B>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>) {
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

class Template3<T: Schema, A, B, C>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>, val c: AbstractColumn<C, T, *>) {
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

fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.insert(statement: () -> Triple<P, A, B>) {
    val tt = statement()
    Session.current().insert(array(Pair(Schema.current<T>().ID, tt.component1()), Pair(a, tt.component2()), Pair(b, tt.component3())))
}

fun <T : TableSchema<P>, P, C> AbstractColumn<C, T, *>.insert(statement: () -> Pair<P, C>) {
    val tt = statement()
    val id: AbstractColumn<P, T, *> = Schema.current<T>().ID // Type inference failure
    Session.current().insert(array(Pair(id, tt.component1()), Pair(this, tt.component2())))
}

class Template4<T: Schema, A, B, C, D>(val a: AbstractColumn<A, T, *>, val b: AbstractColumn<B, T, *>, val c: AbstractColumn<C, T, *>, val d: AbstractColumn<D, T, *>) {
    fun invoke(av: A, bv: B, cv: C, dv: D): Array<Pair<AbstractColumn<*, T, *>, *>> {
        return array(Pair(a, av), Pair(b, bv), Pair(c, cv), Pair(d, dv))
    }

    fun insert(statement: () -> Quadruple<A, B, C, D>) {
        val tt = statement()
        Session.current().insert(array(Pair(a, tt.component1()), Pair(b, tt.component2()), Pair(c, tt.component3()), Pair(d, tt.component4())))
    }
}