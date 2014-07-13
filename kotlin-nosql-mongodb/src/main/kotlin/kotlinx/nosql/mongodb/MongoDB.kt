package kotlinx.nosql.mongodb

import kotlinx.nosql.Database
import kotlinx.nosql.Session
import com.mongodb.MongoClient
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.util.*
import java.util.concurrent.ConcurrentHashMap
import com.mongodb.ServerAddress
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import kotlinx.nosql.DatabaseInitialization
import kotlinx.nosql.Create
import kotlinx.nosql.CreateDrop
import kotlinx.nosql.Validate
import kotlinx.nosql.Update
import kotlinx.nosql.AbstractSchema

fun MongoDB(uri: MongoClientURI, schemas: Array<out AbstractSchema>, initialization: DatabaseInitialization<MongoDBSession> = Validate()): MongoDB {
    val seeds: Array<ServerAddress> = uri.getHosts()!!.map { host ->
        if (host.indexOf(':') > 0) {
            val tokens = host.split(':')
            ServerAddress(tokens[0], tokens[1].toInt())
        } else
            ServerAddress(host)
    }.copyToArray()
    val database: String = if (uri.getDatabase() != null) uri.getDatabase()!! else "test"
    val username: String = if (uri.getUsername() != null) uri.getUsername()!! else ""
    val password: CharArray = if (uri.getPassword() != null) uri.getPassword()!! else CharArray(0)
    val options: MongoClientOptions = uri.getOptions()!!
    return MongoDB(seeds, database, username, password, options, schemas, initialization)
}

class MongoDB(seeds: Array<ServerAddress> = array(ServerAddress()), val database: String = "test", val userName: String = "",
              val password: CharArray = CharArray(0), val options: MongoClientOptions = MongoClientOptions.Builder().build()!!,
              schemas: Array<out AbstractSchema>, initialization: DatabaseInitialization<MongoDBSession> = Validate()) : Database<MongoDBSession>(schemas, initialization) {
    val seeds = seeds
    val db = MongoClient(seeds.toList(), options).getDB(database)!!
    var session = MongoDBSession(db);

    {
        if (userName != "")
            db.authenticate(userName, password)

        initialize()
    }

    // TODO: Use session pool
    override fun <R> withSession(statement: MongoDBSession.() -> R): R {
        Session.threadLocale.set(session)
        val r = session.statement()
        Session.threadLocale.set(null)
        return r
    }
}