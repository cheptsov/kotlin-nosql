package kotlin.nosql.dynamodb

import kotlin.nosql.Database
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import kotlin.nosql.Session
import kotlin.nosql.AbstractSchema

class DynamoDB(val accessKey: String, val secretKey: String, schemas: Array<AbstractSchema>) : Database<DynamoDBSession>(schemas) {
    override fun invoke(statement: DynamoDBSession.() -> Unit) {
        val session = DynamoDBSession(AmazonDynamoDBClient(BasicAWSCredentials(accessKey, secretKey)))
        Session.threadLocale.set(session)
        session.statement()
        Session.threadLocale.set(null)
    }
}