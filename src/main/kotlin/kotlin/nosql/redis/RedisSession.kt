package kotlin.nosql.redis

import kotlin.nosql.Session
import kotlin.nosql.Schema
import kotlin.nosql.Column
import kotlin.nosql.Op
import kotlin.nosql.Query1
import kotlin.nosql.Template2
import kotlin.nosql.Query2
import redis.clients.jedis.Jedis
import kotlin.nosql.PKColumn
import java.util.HashMap
import kotlin.nosql.EqualsOp
import kotlin.nosql.LiteralOp
import kotlin.nosql.ColumnType
import kotlin.nosql.ColumnType.INTEGER
import kotlin.nosql.RangeQuery
import kotlin.nosql.NotFoundException

class RedisSession(val jedis: Jedis) : Session() {
    override fun <T : Schema> T.next(c: T.() -> Column<Int, T>): Int {
        val c = c()
        return jedis.incr(c.table.tableName + ":" + c.name)!!.toInt()
    }
    override fun <T : Schema> Column<Int, T>.add(c: () -> Int): Int {
        return jedis.incrBy(table.tableName + ":" + name, c().toLong())!!.toInt()
    }

    override fun <T : Schema> T.create() {
        // TODO: Do nothing
    }
    override fun <T : Schema> T.drop() {
        // TODO: Do nothing
    }

    override fun <T: Schema, C> T.get(c: T.() -> Column<C, T>): C {
        val c = c()
        val v = jedis.get(c.table.tableName + ":" + c.name)
        if (v != null) {
            return when (c.columnType) {
                ColumnType.INTEGER -> Integer.parseInt(v) as C
                ColumnType.STRING -> v as C
                else -> throw UnsupportedOperationException()
            }
        } else {
            throw NotFoundException(c.table.tableName + ":" + c.name)
        }
    }

    override fun <T: Schema, C> T.set(c: () -> Column<C, T>, v: C) {
        val c = c()
        val v = jedis.set(c.table.tableName + ":" + c.name, c.toString())
    }

    override fun <T : Schema> insert(columns: Array<Pair<Column<out Any?, T>, Any?>>) {
        if (columns.isNotEmpty()) {
            var key: String? = null
            for (column in columns) {
                if (column.component1() is PKColumn<*, *>) {
                    key = column.component2().toString()
                }
            }
            if (key == null) {
                throw IllegalArgumentException()
            }
            val hash = HashMap<String, String>()
            for (column in columns) {
                if (!(column.component1() is PKColumn<*, *>)) {
                    if (column.component1().columnType == ColumnType.INTEGER
                    || column.component1().columnType == ColumnType.STRING) {
                        hash.put(column.component1().name, column.component2().toString())
                    } else if (column.component2() is Set<*>) {
                        for (v in column.component2() as Set<*>) {
                            jedis.sadd(column.component1().table.tableName + ":" + key + ":" + column.component1().name, v.toString())
                        }
                    } else if (column.component2() is List<*>) {
                        for (v in column.component2() as List<*>) {
                            jedis.rpush(column.component1().table.tableName + ":" + key + ":" + column.component1().name, v.toString())
                        }
                    }
                }
            }
            if (key != null) {
                jedis.hmset(columns[0].component1().table.tableName + ":" + key, hash)
            }
        }
    }

    override fun <T : Schema, C> RangeQuery<T, C>.forEach(st: (c: C) -> Unit) {
        val op1 = this.query.op
        if (op1 is EqualsOp && op1.expr1 is PKColumn<*, *>
        && op1.expr2 is LiteralOp) {
            val values = jedis.lrange(op1.expr1.table.tableName + ":" + op1.expr2.value.toString() + ":" + query.a.name, range.start.toLong(), range.end.toLong())
            if (values != null) {
                for (s in values) {
                    st(when (query.a.columnType) {
                        ColumnType.INTEGER_LIST -> Integer.parseInt(s) as C
                        ColumnType.STRING_LIST -> s as C
                        else ->
                            throw UnsupportedOperationException()
                    })
                }
            }
        }
    }

    override fun <T : Schema> delete(table: T, op: Op) {
        throw UnsupportedOperationException()
    }

    override fun <T : Schema, C> Query1<T, C>.set(c: () -> C) {
        throw UnsupportedOperationException()
    }

    override fun <T : Schema> Query1<T, Int>.add(c: () -> Int): Int {
        val op1 = op!!
        if (op1 is EqualsOp && op1.expr1 is PKColumn<*, *>
        && op1.expr2 is LiteralOp) {
            return jedis.hincrBy(op1.expr1.table.tableName + ":" + op1.expr2.value.toString(), a.name, c().toLong())!!.toInt()
        } else {
            throw UnsupportedOperationException()
        }
    }

    override fun <T : Schema, C, CC : Collection<*>> Query1<T, CC>.add(c: () -> C) {
        val op1 = op!!
        if (op1 is EqualsOp && op1.expr1 is PKColumn<*, *>
        && op1.expr2 is LiteralOp) {
            if (a.columnType == ColumnType.INTEGER_LIST
            || a.columnType == ColumnType.STRING_LIST) {
                jedis.rpush(op1.expr1.table.tableName + ":" + op1.expr2.value.toString() + ":" + a.name, c().toString())
            } else {
                jedis.sadd(op1.expr1.table.tableName + ":" + op1.expr2.value.toString() + ":" + a.name, c().toString())
            }
        }
    }

    override fun <T : Schema, C> Column<C, T>.forEach(statement: (C) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : Schema, C> Column<C, T>.iterator(): Iterator<C> {
        throw UnsupportedOperationException()
    }

    override fun <T : Schema, C, M> Column<C, T>.map(statement: (C) -> M): List<M> {
        throw UnsupportedOperationException()
    }

    override fun <T : Schema, C> Column<C, T>.get(op: T.() -> Op): C {
        val where = table.op()
        if (where is EqualsOp && where.expr1 is PKColumn<*, *> && where.expr2 is LiteralOp) {
            if (columnType == ColumnType.INTEGER || columnType == ColumnType.STRING) {
                val v = jedis.hget(where.expr1.table.tableName + ":" + where.expr2.value.toString(), name)
                if (v != null) {
                    when (columnType) {
                        ColumnType.INTEGER -> {
                            return Integer.parseInt(v) as C
                        }
                        ColumnType.STRING -> return v as C
                        else -> throw UnsupportedOperationException()
                    }
                } else {
                    throw NotFoundException(where.expr1.table.tableName + ":" + where.expr2.value.toString() + " " + name)
                }
            } else if (columnType == ColumnType.INTEGER_SET || columnType == ColumnType.STRING_SET) {
                val v = jedis.smembers(where.expr1.table.tableName + ":" + where.expr2.value.toString() + ":" + name)
                if (v != null) {
                    return v.map {
                        when (columnType) {
                            ColumnType.INTEGER_SET -> {
                                Integer.parseInt(it)
                            }
                            ColumnType.STRING_SET -> it
                            else -> throw UnsupportedOperationException()
                        }
                    }.toSet() as C
                } else {
                    throw NotFoundException(where.expr2.value)
                }
            } else {
                throw UnsupportedOperationException()
            }
        } else {
            throw UnsupportedOperationException()
        }
    }

    override fun <T : Schema, A, B> Template2<T, A, B>.get(op: T.() -> Op): Pair<A, B>? {
        throw UnsupportedOperationException()
    }
    override fun <T : Schema, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : Schema, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        throw UnsupportedOperationException()
    }
    override fun <T : Schema, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M> {
        throw UnsupportedOperationException()
    }
    override fun <T : Schema, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit) {

    }
    private fun <C> convert(st: String?, columnType: ColumnType): C {
        if (st == null) {
            return null as C
        } else {
            return when (columnType) {
                ColumnType.INTEGER -> Integer.parseInt(st) as C
                ColumnType.STRING -> st as C
                else -> throw UnsupportedOperationException()
            }
        }
    }
    override fun <T : Schema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit) {
        val op = op!!
        if (op is EqualsOp && op.expr1 is PKColumn<*, *> && op.expr2 is LiteralOp) {
            var av: String?
            var bv: String?
            if (a.columnType == ColumnType.INTEGER || a.columnType == ColumnType.STRING) {
                av = jedis.hget(a.table.tableName + ":" + op.expr2.value.toString(), a.name)
            } else {
                throw UnsupportedOperationException()
            }
            if (b.columnType == ColumnType.INTEGER || b.columnType == ColumnType.STRING) {
                bv = jedis.hget(a.table.tableName + ":" + op.expr2.value.toString(), b.name)
            } else {
                throw UnsupportedOperationException()
            }
            if ((av != null || a._nullable)
            && (bv != null || b._nullable)) {
                statement(convert<A>(av, a.columnType), convert<B>(bv, b.columnType))
            } else {
                throw NullPointerException()
            }
        }
    }
    override fun <T : Schema, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        throw UnsupportedOperationException()
    }

}