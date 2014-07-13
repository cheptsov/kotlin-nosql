package kotlinx.nosql.redis

import kotlinx.nosql.DatabaseInitialization
import kotlinx.nosql.Validate
import kotlinx.nosql.Database
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisShardInfo
import kotlinx.nosql.AbstractSchema

class RedisDB(val password: String = "", schemas: Array<out AbstractSchema>,
              initialization: DatabaseInitialization<RedisDBSession> = Validate()) :
        Database<RedisDBSession>(schemas, initialization) {
    val jedis: Jedis = Jedis("localhost")!!

    {
        if (password != "") {
            jedis.auth(password)
        }

        initialize()
    }

    override fun <R> withSession(statement: RedisDBSession.() -> R): R {
        throw UnsupportedOperationException()
    }
}