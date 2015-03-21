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
import kotlinx.nosql.SchemaGenerationAction
import kotlinx.nosql.Create
import kotlinx.nosql.CreateDrop
import kotlinx.nosql.Validate
import kotlinx.nosql.Update
import kotlinx.nosql.AbstractSchema
import com.mongodb.MongoCredential

fun MongoDB(uri: MongoClientURI, schemas: Array<out AbstractSchema>, initialization: SchemaGenerationAction<MongoDBSession> = Validate()): MongoDB {
    val seeds: Array<ServerAddress> = uri.getHosts()!!.map { host ->
        if (host.indexOf(':') > 0) {
            val tokens = host.split(':')
            ServerAddress(tokens[0], tokens[1].toInt())
        } else
            ServerAddress(host)
    }.copyToArray()
    val database: String = if (uri.getDatabase() != null) uri.getDatabase()!! else "test"
    val options: MongoClientOptions = uri.getOptions()!!
    val credentials = if (uri.getUsername() != null)
      array(MongoCredential.createMongoCRCredential(uri.getUsername(), database, uri.getPassword())!!)
    else array()
  return MongoDB(seeds, database, credentials, options, schemas, initialization)
}

// TODO: Allow use more than one database
class MongoDB(seeds: Array<ServerAddress> = array(ServerAddress()), val database: String = "test",
              val credentials: Array<MongoCredential> = array(), val options: MongoClientOptions = MongoClientOptions.Builder().build()!!,
              schemas: Array<out AbstractSchema>, action: SchemaGenerationAction<MongoDBSession> = Validate()) : Database<MongoDBSession>(schemas, action) {
    val seeds = seeds
    val db = MongoClient(seeds.toList(), credentials.toList(), options).getDB(database)!!
    var session = MongoDBSession(db)

    init {
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
