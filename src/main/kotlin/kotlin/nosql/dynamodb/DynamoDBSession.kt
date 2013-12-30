package kotlin.nosql.dynamodb

import kotlin.nosql.Session
import kotlin.nosql.Table
import kotlin.nosql.Column
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
import kotlin.nosql.PKColumn
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate
import com.amazonaws.services.dynamodb.model.AttributeAction
import com.amazonaws.services.dynamodb.model.DeleteItemRequest
import java.util.Collections
import kotlin.nosql.Query1
import kotlin.nosql.UpdateQuery

class DynamoDBSession(val client: AmazonDynamoDBClient) : Session() {
    override fun <T : Table, C> Column<C, T>.get(op: T.() -> Op): C? {
        val scanRequest = ScanRequest(table.tableName)
                .withAttributesToGet(name)!!.withScanFilter(getScanFilter(table.op()))!!
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            return item.get(name)!! to columnType
        }
        return null
    }

    override fun <T : Table, A, B> Template2<T, A, B>.get(op: T.() -> Op): Pair<A, B>? {
        val scanRequest = ScanRequest(a.table.tableName)
                .withAttributesToGet(a.name, b.name)!!.withScanFilter(getScanFilter(a.table.op()))!!
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            return Pair(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType)
        }
        return null
    }

    override fun <T : Table> delete(table: T, op: Op) {
        if (op is EqualsOp) {
            if (op.expr1 is PKColumn<*, *> && op.expr2 is LiteralOp) {
                var deleteItemRequest = DeleteItemRequest().withTableName(table.tableName)!!
                        .withKey(Key(toAttributeValue((op.expr2 as LiteralOp).value, op.expr1.columnType)))!!
                client.deleteItem(deleteItemRequest)
            }
        }
    }

    private fun <T : Table> update(query: UpdateQuery<T>) {
        if (query.where is EqualsOp && query.where.expr1 is PKColumn<*, *> && query.where.expr2 is LiteralOp) {
            val updates = HashMap<String, AttributeValueUpdate>()
            for (v in query.values) {
                updates.put(v.key.name, AttributeValueUpdate(toAttributeValue(v.value, v.key.columnType), AttributeAction.PUT))
            }
            var updateItemRequest = UpdateItemRequest().withTableName(query.table.tableName)!!
                    .withKey(Key(toAttributeValue((query.where.expr2 as LiteralOp).value, query.where.expr1.columnType)))!!
                    .withAttributeUpdates(updates)
            client.updateItem(updateItemRequest)
        }
    }

    override fun <T : Table, C> Query1<T, C>.set(c: () -> C) {
        val updateQuery = UpdateQuery(a.table, op!!)
        updateQuery.set(a, c())
        update(updateQuery)
    }

    override fun <T : Table, C> Query1<T, Set<C>>.push(c: () -> C) {
        val where = op!!
        if (where is EqualsOp && where.expr1 is PKColumn<*, *> && where.expr2 is LiteralOp) {
            val updates = HashMap<String, AttributeValueUpdate>()
            val v = c()
            updates.put(a.name, AttributeValueUpdate(toAttributeValue(setOf(v), a.columnType), AttributeAction.ADD))
            var updateItemRequest = UpdateItemRequest().withTableName(a.table.tableName)!!
                    .withKey(Key(toAttributeValue((where.expr2 as LiteralOp).value, where.expr1.columnType)))!!
                    .withAttributeUpdates(updates)
            client.updateItem(updateItemRequest)
        }
    }


    private fun <C> AttributeValue.to(columnType: ColumnType): C {
        return when (columnType) {
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
            else -> throw IllegalAccessException()
        }
    }

    private fun toAttributeValue(value: Any, columnType: ColumnType): AttributeValue {
        return when (columnType) {
            ColumnType.INTEGER -> AttributeValue().withN(value.toString())!!
            ColumnType.INTEGER_SET -> AttributeValue().withNS((value as Set<Int>).map { value.toString() })!!
            ColumnType.STRING -> AttributeValue().withS(value as String)!!
            ColumnType.STRING_SET -> AttributeValue().withSS(value as Set<String>)!!
            else ->
                throw IllegalArgumentException()
        }
    }

    override fun <T : Table, C> Column<C, T>.forEach(statement: (C) -> Unit) {
        val scanRequest = ScanRequest(table.tableName)
                .withAttributesToGet(name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            statement(item.get(name)!! to columnType)
        }
    }

    override fun <T : Table, C, M> Column<C, T>.map(statement: (C) -> M): List<M> {
        val results = ArrayList<M>()
        val scanRequest = ScanRequest(table.tableName)
                .withAttributesToGet(name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            results.add(statement(item.get(name)!! to columnType))
        }
        return results
    }

    override fun <T : Table, C> Column<C, T>.iterator(): Iterator<C> {
        return map { it }.iterator()
    }

    override fun <T : Table, A, B> Template2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        val scanRequest = ScanRequest(table.tableName)
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            val av = item.get(a.name)
            val bv = item.get(b.name)
            statement(if (av != null) av to a.columnType else null as A, if (bv != null) bv to b.columnType else null as B)
        }
    }

    override fun <T : Table, A, B, M> Template2<T, A, B>.map(statement: (A, B) -> M): List<M> {
        val results = ArrayList<M>()
        val scanRequest = ScanRequest(table.tableName)
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            results.add(statement(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType))
        }
        return results
    }

    override fun <T : Table, A, B> Template2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        val results = ArrayList<Pair<A, B>>()
        val scanRequest = ScanRequest(table.tableName)
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            results.add(Pair(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType))
        }
        return results.iterator()
    }

    private fun getScanFilter(op: Op): HashMap<String, Condition> {
        val scanFilter = HashMap<String, Condition>()
        if (op is EqualsOp && (op as EqualsOp).expr1 is Column<*, *> && (op as EqualsOp).expr2 is LiteralOp) {
            val condition = Condition().withComparisonOperator(ComparisonOperator.EQ.toString())!!
                    .withAttributeValueList(toAttributeValue(((op as EqualsOp).expr2 as LiteralOp).value, ((op as EqualsOp).expr1 as Column<*, *>).columnType))!!
            scanFilter.put((op as EqualsOp).expr1.name, condition)
        }
        return scanFilter
    }

    override fun <T : Table, A, B> Query2<T, A, B>.forEach(statement: (A, B) -> Unit) {
        val scanRequest = ScanRequest(a.table.tableName).withScanFilter(getScanFilter(op!!))!!
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            statement(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType)
        }
    }
    override fun <T : Table, A, B> Query2<T, A, B>.iterator(): Iterator<Pair<A, B>> {
        val results = ArrayList<Pair<A, B>>()
        val scanRequest = ScanRequest(a.table.tableName).withScanFilter(getScanFilter(op!!))!!
                .withAttributesToGet(a.name, b.name)
        val scanResult = client.scan(scanRequest)!!
        for (item in scanResult.getItems()!!) {
            results.add(Pair(item.get(a.name)!! to a.columnType, item.get(b.name)!! to b.columnType))
        }
        return results.iterator()
    }

    override fun <T : Table> insert(columns: Array<Pair<Column<*, T>, *>>) {
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
        var putItemRequest = PutItemRequest(columns[0].first.table.tableName, item)
        client.putItem(putItemRequest)
    }

    override fun <T : Table> T.drop() {
        try {
            client.deleteTable(DeleteTableRequest().withTableName(tableName))!!.getTableDescription()
        } catch(e: AmazonServiceException) {
            println(e.getMessage())
        }
    }

    override fun <T : Table> T.create() {
        val createTableRequest = CreateTableRequest().withTableName(tableName)!!;
        if (!primaryKeys.isEmpty()) {
            createTableRequest.withKeySchema(KeySchema(KeySchemaElement().withAttributeName(primaryKeys[0].name)!!
                    .withAttributeType(when (primaryKeys[0].columnType) {
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

        System.out.println("Waiting for " + tableName + " to become ACTIVE...")
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
                val describeTableRequest = DescribeTableRequest().withTableName(tableName)
                val tableDescription = client.describeTable(describeTableRequest)!!.getTable()
                val tableStatus = tableDescription!!.getTableStatus()
                if (tableStatus.equals(TableStatus.ACTIVE.toString()))
                    return

            } catch (e: AmazonServiceException) {
                if (e.getErrorCode()!!.equalsIgnoreCase("ResourceNotFoundException") == false)
                    throw e
            }

        }
        throw IllegalStateException("Table " + tableName + " never went status")
    }
}
