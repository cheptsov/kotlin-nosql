package kotlin.nosql

import java.sql.Connection
import java.util.HashSet
import java.util.ArrayList

open class Query<T>(val session: Session, val fields: Array<Field<*>>) {
    private var op: Op? = null;
    private var selectedTables = ArrayList<Table>();
    private var joins = ArrayList<Column<*, *>>();
    private var selectedColumns = HashSet<Column<*, *>>();
    private var groupedByColumns = ArrayList<Column<*, *>>();

    fun from (vararg tables: Table) : Query<T> {
        for (table in tables) {
            selectedTables.add(table)
        }
        return this
    }

    fun join (vararg foreignKeys: Column<*, *>): Query<T> {
        for (foreignKey in foreignKeys) {
            joins.add(foreignKey)
        }
        return this
    }

    open fun where(op: Op): Query<T> {
        this.op = op
        return this
    }

    fun or(op: Op): Query<T> {
        this.op = OrOp(this.op!!, op)
        return this
    }

    fun and(op: Op): Query<T> {
        this.op = AndOp(this.op!!, op)
        return this
    }

    fun groupBy(vararg columns: Column<*, *>): Query<T> {
        for (column in columns) {
            groupedByColumns.add(column)
        }
        return this
    }

    fun <B> map(statement: (row: T) -> B): List<B> {
        val results = ArrayList<B>()
        forEach {
            results.add(statement(it))
        }
        return results
    }

    fun joined(column: Column<*, *>): Boolean {
        for (foreignKey in joins) {
            if (foreignKey is FKColumn<*, *> && foreignKey.reference.table == column.table) {
                return true
            } else if (foreignKey is FKOptionColumn<*, *> && foreignKey.reference.table == column.table) {
                return true
            }
        }
        return false
    }

    fun forEach(statement: (row: T) -> Unit) {
        val tables: MutableSet<Table> = HashSet<Table>()
        val sql = StringBuilder("SELECT ")
        if (fields.size > 0) {
            var c = 0;
            for (field in fields) {
                if (field is Column<*, *>) {
                    selectedColumns.add(field)
                    if (!joined(field)) {
                        tables.add(field.table)
                    }
                } else if (field is Function<*>) {
                    for (column in field.columns) {
                        selectedColumns.add(column)
                        if (!joined(column)) {
                            tables.add(column.table)
                        }
                    }
                }
                sql.append(field.toSQL())
                c++
                if (c < fields.size) {
                    sql.append(", ")
                }
            }
        }
        sql.append(" FROM ")
        var c = 0;
        if (selectedTables.isEmpty()) {
            for (table in tables) {
                sql.append(session.identity(table))
                c++
                if (c < tables.size) {
                    sql.append(", ")
                }
            }
        } else {
            for (table in selectedTables) {
                sql.append(session.identity(table))
                c++
                if (c < selectedTables.size) {
                    sql.append(", ")
                }
            }
        }
        for (join in joins) {
            if (join is FKColumn<*, *>) {
                val primaryKey = join.reference.table.primaryKeys[0]
                sql.append(" INNER JOIN ").append(session.identity(join.reference.table)).append(" ON ").
                append(session.fullIdentity(join)).append(" = ").append(session.fullIdentity(primaryKey))
            } else if (join is FKOptionColumn<*, *>) {
                val primaryKey = join.reference.table.primaryKeys[0]
                sql.append(" LEFT JOIN ").append(session.identity(join.reference.table)).append(" ON ").
                append(session.fullIdentity(join)).append(" = ").append(session.fullIdentity(primaryKey))
            }
        }
        if (op != null) {
            sql.append(" WHERE ").append(op!!.toSQL())
        }
        if (groupedByColumns.size > 0) {
            sql.append(" GROUP BY ")
        }
        c = 0;
        for (column in groupedByColumns) {
            sql.append(session.fullIdentity(column))
            c++
            if (c < groupedByColumns.size) {
                sql.append(", ")
            }
        }
        println("SQL: " + sql.toString())
        val rs = session.connection.createStatement()?.executeQuery(sql.toString())!!
        while (rs.next()) {
            if (fields.size == 1) {
                statement(rs.getObject(1) as T)
            } else if (fields.size == 2) {
                statement(Pair(rs.getObject(1), rs.getObject(2)) as T)
            } else if (fields.size == 3) {
                statement(Triple(rs.getObject(1), rs.getObject(2), rs.getObject(3)) as T)
            } else if (fields.size == 4) {
                statement(Quadruple(rs.getObject(1), rs.getObject(2), rs.getObject(3), rs.getObject(4)) as T)
            } else if (fields.size == 5) {
                statement(Quintuple(rs.getObject(1), rs.getObject(2), rs.getObject(3), rs.getObject(4), rs.getObject(5)) as T)
            }
        }
    }
}

class Query2<A, B>(session: Session, a: Field<A>, b: Field<B>): Query<Pair<A, B>>(session, array(a, b)) {
    fun forEach(statement: (a: A, b: B) -> Unit) {
        super.forEach {
            val (a, b) = it
            statement(a, b);
        }
    }

    fun iterator() : Iterator<Pair<A, B>> {
        val results = ArrayList<Pair<A, B>>()
        forEach { a, b ->
            results.add(Pair(a, b))
        }
        return results.iterator()
    }

    override fun where(op: Op): Query2<A, B> {
        return super.where(op) as Query2<A, B>
    }
}
