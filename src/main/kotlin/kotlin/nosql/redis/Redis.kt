package kotlin.nosql.redis

import kotlin.nosql.Database
import redis.clients.jedis.Jedis
import kotlin.nosql.Session
import kotlin.nosql.AbstractSchema

class Redis(val host: String, schemas: Array<AbstractSchema>) : Database<RedisSession>(schemas) {
    override fun invoke(statement: RedisSession.() -> Unit) {
        val session = RedisSession(Jedis(host))
        Session.threadLocale.set(session)
        session.statement()
        Session.threadLocale.set(null)
    }
}