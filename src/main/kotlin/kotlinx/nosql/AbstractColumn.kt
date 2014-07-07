package kotlinx.nosql

import java.util.ArrayList
import java.util.regex.Pattern
import rx.Observable
import rx.Observable.OnSubscribeFunc
import rx.subscriptions.Subscriptions

open class AbstractColumn<C, T : AbstractSchema, S>(val name: String, val valueClass: Class<S>, val columnType: ColumnType) : ColumnObservable<C>(), Expression<C> {
    fun matches(other: Pattern): Op {
        return MatchesOp(this, LiteralOp(other))
    }

    override fun toString(): String {
        return "$name"
    }

    fun <C2> plus(c: AbstractColumn<C2, T, *>): Template2<T, C, C2> {
        return Template2(this, c) as Template2<T, C, C2>
    }
}

abstract class ColumnObservable<C>: Observable<C>(OnSubscribeFunc<C> { observer ->
    val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
    tableSchemaProjectionQueryObservable.subscribe(onNext = { observer.onNext(it as C) }, onError = { observer.onError(it) }, onComplete = { observer.onCompleted() })
    Subscriptions.empty()!!
}) {
}

fun <C, T: AbstractSchema> AbstractColumn<C, T, *>.update(value: C): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to value),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B> ColumnObservable<Pair<A, B>>.update(a: A, b: B): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.addAll(values: S): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(addAll(tableSchemaProjectionQueryObservable.params.table,
                        tableSchemaProjectionQueryObservable.params.projection.get(0)
                                as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, values,
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.add(value: C): Observable<Int> {
    val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
    val values: Collection<C> = if (tableSchemaProjectionQueryObservable.params.projection.get(0).columnType.list) listOf(value) else setOf(value)
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(addAll(tableSchemaProjectionQueryObservable.params.table,
                        tableSchemaProjectionQueryObservable.params.projection.get(0)
                                as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, values,
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.removeAll(values: S): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(removeAll(tableSchemaProjectionQueryObservable.params.table,
                        tableSchemaProjectionQueryObservable.params.projection.get(0)
                                as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, values,
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <C> AbstractColumn<out Collection<C>, *, *>.remove(value: C): Observable<Int> {
    val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
    val values: Collection<C> = if (tableSchemaProjectionQueryObservable.params.projection.get(0).columnType.list) listOf(value) else setOf(value)
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(removeAll(tableSchemaProjectionQueryObservable.params.table,
                        tableSchemaProjectionQueryObservable.params.projection.get(0)
                                as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, values,
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <C: AbstractColumn<out Collection<*>, *, *>> C.remove(removeOp: C.() -> Op): Observable<Int> {
    val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
    return Observable.create(OnSubscribeFunc { observer ->
        val removeOpValue = with (tableSchemaProjectionQueryObservable.params.projection.get(0)) {
            removeOp()
        }
        with (Session.current()) {
            try {
                observer.onNext(removeAll(tableSchemaProjectionQueryObservable.params.table,
                        tableSchemaProjectionQueryObservable.params.projection.get(0)
                                as AbstractColumn<Collection<C>, out AbstractSchema, out Any?>, removeOpValue,
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B, C> ColumnObservable<Triple<A, B, C>>.update(a: A, b: B, c: C): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b,
                        tableSchemaProjectionQueryObservable.params.projection.get(2) to c),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B, C, D> ColumnObservable<Quadruple<A, B, C, D>>.update(a: A, b: B, c: C, d: D): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b,
                        tableSchemaProjectionQueryObservable.params.projection.get(2) to c,
                        tableSchemaProjectionQueryObservable.params.projection.get(3) to d),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B, C, D, E> ColumnObservable<Quintuple<A, B, C, D, E>>.update(a: A, b: B, c: C, d: D, e: E): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b,
                        tableSchemaProjectionQueryObservable.params.projection.get(2) to c,
                        tableSchemaProjectionQueryObservable.params.projection.get(3) to d,
                        tableSchemaProjectionQueryObservable.params.projection.get(4) to e),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B, C, D, E, F> ColumnObservable<Sextuple<A, B, C, D, E, F>>.update(a: A, b: B, c: C, d: D, e: E, f: F): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b,
                        tableSchemaProjectionQueryObservable.params.projection.get(2) to c,
                        tableSchemaProjectionQueryObservable.params.projection.get(3) to d,
                        tableSchemaProjectionQueryObservable.params.projection.get(4) to e,
                        tableSchemaProjectionQueryObservable.params.projection.get(5) to f),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B, C, D, E, F, G> ColumnObservable<Septuple<A, B, C, D, E, F, G>>.update(a: A, b: B, c: C, d: D, e: E, f: F, g: G): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b,
                        tableSchemaProjectionQueryObservable.params.projection.get(2) to c,
                        tableSchemaProjectionQueryObservable.params.projection.get(3) to d,
                        tableSchemaProjectionQueryObservable.params.projection.get(4) to e,
                        tableSchemaProjectionQueryObservable.params.projection.get(5) to f,
                        tableSchemaProjectionQueryObservable.params.projection.get(6) to g),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B, C, D, E, F, G, H> ColumnObservable<Octuple<A, B, C, D, E, F, G, H>>.update(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b,
                        tableSchemaProjectionQueryObservable.params.projection.get(2) to c,
                        tableSchemaProjectionQueryObservable.params.projection.get(3) to d,
                        tableSchemaProjectionQueryObservable.params.projection.get(4) to e,
                        tableSchemaProjectionQueryObservable.params.projection.get(5) to f,
                        tableSchemaProjectionQueryObservable.params.projection.get(6) to g,
                        tableSchemaProjectionQueryObservable.params.projection.get(7) to h),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B, C, D, E, F, G, H, J> ColumnObservable<Nonuple<A, B, C, D, E, F, G, H, J>>.update(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, j: J): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b,
                        tableSchemaProjectionQueryObservable.params.projection.get(2) to c,
                        tableSchemaProjectionQueryObservable.params.projection.get(3) to d,
                        tableSchemaProjectionQueryObservable.params.projection.get(4) to e,
                        tableSchemaProjectionQueryObservable.params.projection.get(5) to f,
                        tableSchemaProjectionQueryObservable.params.projection.get(6) to g,
                        tableSchemaProjectionQueryObservable.params.projection.get(7) to h,
                        tableSchemaProjectionQueryObservable.params.projection.get(8) to j),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}

fun <A, B, C, D, E, F, G, H, J, K> ColumnObservable<Decuple<A, B, C, D, E, F, G, H, J, K>>.update(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, j: J, k: K): Observable<Int> {
    return Observable.create(OnSubscribeFunc { observer ->
        val tableSchemaProjectionQueryObservable = tableSchemaProjectionObservableThreadLocale.get()!!
        with (Session.current()) {
            try {
                observer.onNext(update(tableSchemaProjectionQueryObservable.params.table, array(tableSchemaProjectionQueryObservable.params.projection.get(0) to a,
                        tableSchemaProjectionQueryObservable.params.projection.get(1) to b,
                        tableSchemaProjectionQueryObservable.params.projection.get(2) to c,
                        tableSchemaProjectionQueryObservable.params.projection.get(3) to d,
                        tableSchemaProjectionQueryObservable.params.projection.get(4) to e,
                        tableSchemaProjectionQueryObservable.params.projection.get(5) to f,
                        tableSchemaProjectionQueryObservable.params.projection.get(6) to g,
                        tableSchemaProjectionQueryObservable.params.projection.get(7) to h,
                        tableSchemaProjectionQueryObservable.params.projection.get(8) to j,
                        tableSchemaProjectionQueryObservable.params.projection.get(9) to k),
                        tableSchemaProjectionQueryObservable.params.query!!))
                observer.onCompleted();
            } catch (e: Throwable) {
                observer.onError(e)
            }
        }
        Subscriptions.empty()!!
    });
}


fun <T : AbstractSchema, C> AbstractColumn<C?, T, C>.isNull(): Op {
    return NullOp(this)
}

fun text(search: String): Op {
    return TextOp(search)
}

fun <T : AbstractSchema, C> AbstractColumn<C?, T, C>.notNull(): Op {
    return NotNullOp(this)
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.equal(other: C): Op {
    return EqualsOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notEqual(other: C): Op {
    return NotEqualsOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Iterable<C>): Op {
    return InOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Array<C>): Op {
    return InOp(this, LiteralOp(other))
}

// TODO TODO TODO: Expression should be typed
fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Expression<out Iterable<C>>): Op {
    return InOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Iterable<C>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Array<C>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Expression<out Iterable<C>>): Op {
    return NotInOp(this, LiteralOp(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, C>.equal(other: Expression<out C?>): Op {
    return EqualsOp(this, other)
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, C>.notEqual(other: Expression<out C?>): Op {
    return NotEqualsOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.gt(other: Expression<out Int?>): Op {
    return GreaterOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.gt(other: Int): Op {
    return GreaterOp(this, LiteralOp(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.ge(other: Expression<out Int?>): Op {
    return GreaterEqualsOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.ge(other: Int): Op {
    return GreaterEqualsOp(this, LiteralOp(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.le(other: Expression<out Int?>): Op {
    return LessEqualsOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.le(other: Int): Op {
    return LessEqualsOp(this, LiteralOp(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.lt(other: Expression<out Int?>): Op {
    return LessOp(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.lt(other: Int): Op {
    return LessOp(this, LiteralOp(other))
}

abstract class Column<C, S : AbstractSchema>(name: String, valueClass: Class<C>, columnType: ColumnType = ColumnType.CUSTOM_CLASS) : AbstractColumn<C, S, C>(name, valueClass, columnType) {
}

trait AbstractNullableColumn {
}

open class NullableIdColumn<I, S : TableSchema<I>, R: TableSchema<I>> (name: String, valueClass: Class<I>,
                                                  columnType: ColumnType) : AbstractColumn<Id<I, R>?, S, I>(name, valueClass, columnType), AbstractNullableColumn {
}

open class NullableColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>,
                                                  columnType: ColumnType) : AbstractColumn<C?, S, C>(name, valueClass, columnType), AbstractNullableColumn {
}

open class SetColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>) : AbstractColumn<Set<C>, S, C>(name, valueClass, ColumnType.CUSTOM_CLASS_SET) {
}

open class ListColumn<C, S : AbstractSchema> (name: String, valueClass: Class<C>) : AbstractColumn<List<C>, S, C>(name, valueClass, ColumnType.CUSTOM_CLASS_LIST) {
}