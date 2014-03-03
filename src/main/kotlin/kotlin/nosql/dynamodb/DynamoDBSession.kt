package kotlin.nosql.dynamodb

import kotlin.nosql.Session
import kotlin.nosql.AbstractTableSchema
import kotlin.nosql.AbstractColumn
import kotlin.nosql.Op
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model.CreateTableRequest
import com.amazonaws.services.dynamodb.model.KeySchema
import com.amazonaws.services.dynamodb.model.KeySchemaElement
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput
import com.amazonaws.services.dynamodb.model.DescribeTableRequest
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.dynamodb.model.TableStatus
import com.amazonaws.services.dynamodb.model.DeleteTableRequest
import com.amazonaws.services.dynamodb.model.PutItemRequest
import java.util.HashMap
import com.amazonaws.services.dynamodb.model.AttributeValue
import kotlin.nosql.ColumnType
import com.amazonaws.services.dynamodb.model.Condition
import com.amazonaws.services.dynamodb.model.ScanRequest
import kotlin.nosql.Query2
import kotlin.nosql.Template2
import java.util.ArrayList
import kotlin.nosql.EqualsOp
import kotlin.nosql.LiteralOp
import com.amazonaws.services.dynamodb.model.ComparisonOperator
import com.amazonaws.services.dynamodb.model.UpdateItemRequest
import kotlin.nosql.PrimaryKeyColumn
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate
import com.amazonaws.services.dynamodb.model.AttributeAction
import com.amazonaws.services.dynamodb.model.DeleteItemRequest
import java.util.Collections
import kotlin.nosql.Query1
import kotlin.nosql.UpdateQuery
import kotlin.nosql.RangeQuery
import kotlin.nosql.NotFoundException
import kotlin.nosql.KeyValueSchema
import kotlin.nosql.AbstractSchema
import kotlin.nosql.DocumentSchema
import kotlin.nosql.TableSchema

class DynamoDBSession(val client: AmazonDynamoDBClient) : Session() {
    override fun <T : DocumentSchema<P, V>, P, V> T.insert(v: () -> V): P {
        throw UnsupportedOperationException()
    }
    override fun <T: DocumentSchema<P, C>, P, C> T.filter(op: T.() -> Op): Iterator<C> {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.get(statement: (A, B) -> Unit) {
        throw UnsupportedOperationException()
    }
    override fun <T : KeyValueSchema> T.next(c: T.() -> AbstractColumn<Int, T, *>): Int {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema> AbstractColumn<Int, T, *>.add(c: () -> Int): Int {
        throw UnsupportedOperationException()
    }
    override fun <T : KeyValueSchema, C> T.set(c: () -> AbstractColumn<C, T, *>, v: C) {
        throw UnsupportedOperationException()
    }
    override fun <T : KeyValueSchema, C> T.get(c: T.() -> AbstractColumn<C, T, *>): C {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema> Query1<T, Int>.add(c: () -> Int): Int {
        throw UnsupportedOperationException()
    }
    override fun <T : AbstractTableSchema, C, CC: Collection<*>> Query1<T, CC>.add(c: () -> C) {
        val table = AbstractSchema.current<T>()
        val where = op!!
        if (where is EqualsOp && where.expr1 is PrimaryKeyColumn<*, *> && where.expr2 is LiteralOp &&
        (a.columnType == ColumnType.INTEGER_SET || a.columnType == ColumnType.STRING_SET )) {
            val updates = HashMap<String, AttributeValueUpdate>()
            val v = c()
            updates.put(a.name, AttributeValueUpdate(toAttributeValue(setOf(v), a.columnType), AttributeAction.ADD))
            var updateItemRequest = UpdateItemRequest().withTableName(table.name)!!
                    .withKey(Key(toAttributeValue((where.expr2 as LiteralOp).value, where.expr1.columnType)))!!
                    .withAttributeUpdates(updates)
            client.updateItem(updateItemRequest)
        } else {
            throw UnsupportedOperationException()
        }
    }

    override fun <T : AbstractTableSchema, C> RangeQuery<T, C>.forEach(st: (C) -> Unit) {
        throw UnsupportedOperationException()
    }

    override fun <T : TableSchema<P>, P, C> AbstractColumn<C, T, *>.get(id: () -> P): C {
        val table = AbstractSchema.current<T>()
        val scanRequest = ScanRequest(table.name)
                .withAttributesToGet(name)!!.withScanFilter(getScanFilter(table.pk eq id()))!!
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            return item.get(name)!! to columnType
        }
        throw NotFoundException("<todo>")
    }

    override fun <T : TableSchema<P>, P, A, B> Template2<T, A, B>.get(id: () -> P): Pair<A, B> {
        val table = AbstractSchema.current<T>()
        val scanRequest = ScanRequest(table.name)
                .withAttributesToGet(a.name, b.name)!!.withScanFilter(getScanFilter(table.pk eq id()))!!
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            return Pair(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType)
        }
        throw NullPointerException()
    }

    override fun <T : AbstractSchema> delete(table: T, op: Op) {
        if (op is EqualsOp) {
            if (op.expr1 is PrimaryKeyColumn<*, *> && op.expr2 is LiteralOp) {
                var deleteItemRequest = DeleteItemRequest().withTableName(table.name)!!
                        .withKey(Key(toAttributeValue((op.expr2 as LiteralOp).value, op.expr1.columnType)))!!
                client.deleteItem(deleteItemRequest)
            }
        }
    }

    private fun <T : AbstractTableSchema> update(query: UpdateQuery<T>) {
        if (query.where is EqualsOp && query.where.expr1 is PrimaryKeyColumn<*, *> && query.where.expr2 is LiteralOp) {
            val updates = HashMap<String, AttributeValueUpdate>()
            for (v in query.values) {
                updates.put(v.key.name, AttributeValueUpdate(toAttributeValue(v.value, v.key.columnType), AttributeAction.PUT))
            }
            var updateItemRequest = UpdateItemRequest().withTableName(query.table.name)!!
                    .withKey(Key(toAttributeValue((query.where.expr2 as LiteralOp).value, query.where.expr1.columnType)))!!
                    .withAttributeUpdates(updates)
            client.updateItem(updateItemRequest)
        }
    }

    override fun <T : AbstractTableSchema, C> Query1<T, C>.set(c: () -> C) {
        val updateQuery = UpdateQuery(AbstractSchema.current<T>(), op!!)
        updateQuery.set(a, c())
        update(updateQuery)
    }

    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.set(c: () -> Pair<A, B>) {
        throw UnsupportedOperationException()
    }

    private fun <C> AttributeValue.to(attributeType: ColumnType): C {
        return when (attributeType) {
            ColumnType.INTEGER -> {
                (if (getN() != null) Integer.parseInt(getN()!!) else null) as C
            }
            ColumnType.INTEGER_SET -> {
                val values = getNS()
                if (values != null) values!!.map { Integer.parseInt(it)!! }.toSet() as C else Collections.emptySet<Integer>() as C
            }
            ColumnType.STRING -> getS() as C
            ColumnType.STRING_SET -> {
                val values = getSS()
                if (values != null) values!!.toSet() as C else Collections.emptySet<String>() as C
            }
            else -> throw UnsupportedOperationException()
        }
    }

    private fun toAttributeValue(value: Any, attributeType: ColumnType): AttributeValue {
        return when (attributeType) {
            ColumnType.INTEGER -> AttributeValue().withN(value.toString())!!
            ColumnType.INTEGER_SET -> AttributeValue().withNS((value as Set<Int>).map { value.toString() })!!
            ColumnType.STRING -> AttributeValue().withS(value as String)!!
            ColumnType.STRING_SET -> AttributeValue().withSS(value as Set<String>)!!
            else ->
                throw IllegalArgumentException()
        }
    }

    override fun <T : AbstractTableSchema, C> AbstractColumn<C, T, *>.forEach(statement: (C) -> Unit) {
        val table = AbstractSchema.current<T>()
        val scanRequest = ScanRequest(table.name)
                .withAttributesToGet(name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            statement(item.get(name)!! to columnType)
        }
    }

    override fun <T : AbstractTableSchema, C, M> AbstractColumn<C, T, *>.map(statement: (C) -> M): List<M> {
        val table = AbstractSchema.current<T>()
        val results = ArrayList<M>()
        val scanRequest = ScanRequest(table.name)
                .withAttributesToGet(name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            results.add(statement(item.get(name)!! to columnType))
        }
        return results
    }

    override fun <T : AbstractTableSchema, C> AbstractColumn<C, T, *>.iterator(): Iterator<C> {
        return map { it }.iterator()
    }

    override fun <T : AbstractTableSchema, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        val table = AbstractSchema.current<T>()
        val scanRequest = ScanRequest(table.name)
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            val av = item.get(a.name)
            val bv = item.get(b.name)
            statement(if (av != null) av to a.columnType else null as A, if (bv != null) bv to b.columnType else null as B)
        }
    }

    override fun <T : AbstractTableSchema, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M> {
        val table = AbstractSchema.current<T>()
        val results = ArrayList<M>()
        val scanRequest = ScanRequest(table.name)
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            results.add(statement(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType))
        }
        return results
    }

    override fun <T : AbstractTableSchema, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        val table = AbstractSchema.current<T>()
        val results = ArrayList<Pair<A, B>>()
        val scanRequest = ScanRequest(table.name)
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            results.add(Pair(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType))
        }
        return results.iterator()
    }

    private fun getScanFilter(op: Op): HashMap<String, Condition> {
        val scanFilter = HashMap<String, Condition>()
        if (op is EqualsOp && (op as EqualsOp).expr1 is AbstractColumn<*, *, *> && (op as EqualsOp).expr2 is LiteralOp) {
            val condition = Condition().withComparisonOperator(ComparisonOperator.EQ.toString())!!
                    .withAttributeValueList(toAttributeValue(((op as EqualsOp).expr2 as LiteralOp).value, ((op as EqualsOp).expr1 as AbstractColumn<*, *, *>).columnType))!!
            scanFilter.put((op as EqualsOp).expr1.name, condition)
        } else {
            throw UnsupportedOperationException()
        }
        return scanFilter
    }

    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        val table = AbstractSchema.current<T>()
        val scanRequest = ScanRequest(table.name).withScanFilter(getScanFilter(op!!))!!
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            statement(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType)
        }
    }
    override fun <T : AbstractTableSchema, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        val table = AbstractSchema.current<T>()
        val results = ArrayList<Pair<A, B>>()
        val scanRequest = ScanRequest(table.name).withScanFilter(getScanFilter(op!!))!!
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            results.add(Pair(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType))
        }
        return results.iterator()
    }

    override fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<*, T, *>, *>>) {
        val table = AbstractSchema.current<T>()
        // TODO Clean everything first
        val item = HashMap<String, AttributeValue>()
        for (column in columns) {
            if (column.second != null && !(column.second is Set<*> && (column.second as Set<Any?>).empty)) {
                item.put(column.first.name, when (column.first.columnType) {
                    ColumnType.INTEGER -> AttributeValue().withN(column.second.toString())!!
                    ColumnType.INTEGER_SET -> {
                        val values = column.second as Set<Int>
                        AttributeValue().withNS(values.map { it.toString() })!!
                    }
                    ColumnType.STRING -> AttributeValue().withS(column.second as String)!!
                    ColumnType.STRING_SET -> {
                        val values = column.second as Set<String>
                        AttributeValue().withSS(values)!!
                    }
                    else ->
                        throw IllegalArgumentException()
                })
            }
        }
        var putItemRequest = PutItemRequest(table.name, item)
        client.putItem(putItemRequest)
    }

    override fun <T : AbstractTableSchema> T.drop() {
        try {
            client.deleteTable(DeleteTableRequest().withTableName(name))!!.getTableDescription()
        } catch(e: AmazonServiceException) {
            println(e.getMessage())
        }
    }

    override fun <T : AbstractTableSchema> T.create() {
        val createTableRequest = CreateTableRequest().withTableName(name)!!;
        if (this is TableSchema<*>) {
            createTableRequest.withKeySchema(KeySchema(KeySchemaElement().withAttributeName(pk.name)!!
                    .withAttributeType(when (pk.columnType) {
                ColumnType.STRING -> "S"
                ColumnType.INTEGER -> "N"
                else ->
                    throw IllegalArgumentException()
            })))!!
        }
        createTableRequest.withProvisionedThroughput(ProvisionedThroughput().withReadCapacityUnits(10L)!!
                .withWriteCapacityUnits(10L));
        try {
            client.createTable(createTableRequest)!!.getTableDescription()
        } catch (e: AmazonServiceException) {
            println(e.getMessage())
            return
        }

        System.out.println("Waiting for " + name + " to become ACTIVE...")
        val startTime = System.currentTimeMillis()
        val endTime = startTime + (10 * 60 * 1000).toLong()
        while (System.currentTimeMillis() < endTime)
        {
            try {
                Thread.sleep(1000 * 20.toLong())
            } catch (e: Exception) {
                // ignore
            }
            try {
                val describeTableRequest = DescribeTableRequest().withTableName(name)
                val tableDescription = client.describeTable(describeTableRequest)!!.getTable()
                val tableStatus = tableDescription!!.getTableStatus()
                if (tableStatus.equals(TableStatus.ACTIVE.toString()))
                    return

            } catch (e: AmazonServiceException) {
                if (e.getErrorCode()!!.equalsIgnoreCase("ResourceNotFoundException") == false)
                    throw e
            }

        }
        throw IllegalStateException("Table " + name + " never went status")
    }
}
