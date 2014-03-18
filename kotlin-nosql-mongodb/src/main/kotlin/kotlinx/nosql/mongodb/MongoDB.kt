package kotlinx.nosql.mongodb

import kotlinx.nosql.Database
import kotlinx.nosql.Session
import com.mongodb.MongoClient
import kotlinx.nosql.Schema
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.util.*
import java.util.concurrent.ConcurrentHashMap
import com.mongodb.ServerAddress
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI

fun MongoDB(uri: MongoClientURI, schemas: Array<Schema>): MongoDB {
    val seeds: Array<ServerAddress> = uri.getHosts()!!.map { host ->
        if (host.indexOf(':') > 0) {
            val tokens = host.split(':')
            ServerAddress(tokens[0], tokens[1].toInt())
        } else
            ServerAddress(host)
    }.copyToArray()
    val database: String = if (uri.getDatabase() != null) uri.getDatabase()!! else "test"
    val username: String = if (uri.getUsername() != null) uri.getUsername()!! else ""
    val password: String = if (uri.getPassword() != null) uri.getPassword().toString() else ""
    val options: MongoClientOptions = uri.getOptions()!!
    return MongoDB(seeds, database, username, password, options, schemas)
}

class MongoDB(seeds: Array<ServerAddress> = array(ServerAddress()), val database: String = "test", val userName: String = "",
              val password: String = "", val options: MongoClientOptions = MongoClientOptions.Builder().build()!!,
              schemas: Array<Schema>) : Database<MongoDBSession>(schemas) {
    val seeds = seeds

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
        val db = MongoClient(seeds.toList(), options).getDB(database)!!
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