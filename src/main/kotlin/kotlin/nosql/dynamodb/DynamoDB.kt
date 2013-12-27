package kotlin.nosql.dynamodb

import kotlin.nosql.Database
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import kotlin.nosql.Session

class DynamoDB(accessKey: String = "", secretKey: String = "") : Database<DynamoDBSession>() {
    private val credentials = PropertiesCredentials(
            javaClass<AmazonDynamoDBSample>().getResourceAsStream("AwsCredentials.properties"))

    val accessKey: String = if (accessKey != "") accessKey else credentials.getAWSAccessKeyId()!!
    val secretKey: String = if (secretKey != "") secretKey else credentials.getAWSSecretKey()!!

    override fun invoke(statement: DynamoDBSession.() -> Unit) {
        val session = DynamoDBSession(AmazonDynamoDBClient(BasicAWSCredentials(this.accessKey, this.secretKey)))
        Session.threadLocale.set(session)
        session.statement()
        Session.threadLocale.set(null)
    }
}