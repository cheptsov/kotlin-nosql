package kotlinx.nosql.redis

import kotlinx.nosql.SchemaGenerationAction
import kotlinx.nosql.Validate
import kotlinx.nosql.Database
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisShardInfo
import kotlinx.nosql.AbstractSchema
import kotlinx.nosql.Session

class Redis(val password: String = "", schemas: Array<out AbstractSchema>,
              action: SchemaGenerationAction<RedisSession> = Validate()) :
        Database<RedisSession>(schemas, action) {
    val jedis: Jedis = Jedis("localhost")
    var session = RedisSession(jedis)


    init {
        if (password != "") {
            jedis.auth(password)
        }

        initialize()
    }

  // TODO: Use session pool
  override fun <R> withSession(statement: RedisSession.() -> R): R {
    Session.threadLocale.set(session)
    val r = session.statement()
    Session.threadLocale.set(null)
    return r
  }
}
