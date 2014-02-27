package kotlin.nosql.mongodb

import kotlin.nosql.Database
import kotlin.nosql.Session
import com.mongodb.MongoClient
import kotlin.nosql.AbstractSchema

class MongoDB(val host: String = "localhost", val database: String, val userName: String = "",
              val password: String = "", schemas: Array<AbstractSchema>) : Database<MongoDBSession>() {
    override fun invoke(statement: MongoDBSession.() -> Unit) {
        val db = MongoClient(host).getDB(database)!!
        if (userName != "")
            db.authenticate(userName, password.toCharArray())
        val session = MongoDBSession(db)
        Session.threadLocale.set(session)
        session.statement()
        Session.threadLocale.set(null)
    }
}