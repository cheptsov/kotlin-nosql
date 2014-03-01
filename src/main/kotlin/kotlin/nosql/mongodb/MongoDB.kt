package kotlin.nosql.mongodb

import kotlin.nosql.Database
import kotlin.nosql.Session
import com.mongodb.MongoClient
import kotlin.nosql.AbstractSchema
import kotlin.nosql.AbstractColumn
import kotlin.nosql.util.*
import java.util.concurrent.ConcurrentHashMap

class MongoDB(val host: String = "localhost", val database: String, val userName: String = "",
              val password: String = "", schemas: Array<AbstractSchema>) : Database<MongoDBSession>(schemas) {
    {
        for (schema in schemas) {
            buildFullColumnNames(schema)
        }
    }

    private fun buildFullColumnNames(schema: Any, path: String = "") {
        val fields = getAllFields(schema.javaClass)
        for (field in fields) {
            if (field.isColumn) {
                val column = field.asColumn(schema)
                val columnFullName = path + (if (path.isNotEmpty()) "." else "") + column.name
                fullColumnNames.put(column, columnFullName)
                buildFullColumnNames(column, columnFullName)
            }
        }
    }

    class object {
        val fullColumnNames = ConcurrentHashMap<AbstractColumn<*, *, *>, String>()
    }

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

val AbstractColumn<*, *, *>.fullName: String
    get() {
        return MongoDB.fullColumnNames.get(this)!!
    }