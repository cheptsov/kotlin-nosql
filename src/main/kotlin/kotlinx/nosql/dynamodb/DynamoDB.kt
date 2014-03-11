package kotlinx.nosql.dynamodb

/*
class DynamoDB(val accessKey: String, val secretKey: String, schemas: Array<Schema>) : Database<DynamoDBSession>(schemas) {
    override fun invoke(statement: DynamoDBSession.() -> Unit) {
        val session = DynamoDBSession(AmazonDynamoDBClient(BasicAWSCredentials(accessKey, secretKey)))
        Session.threadLocale.set(session)
        session.statement()
        Session.threadLocale.set(null)
    }
}*/
