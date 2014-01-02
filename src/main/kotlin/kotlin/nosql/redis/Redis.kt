package kotlin.nosql.redis

import kotlin.nosql.Database
import redis.clients.jedis.Jedis
import kotlin.nosql.Session

class Redis(val host: String) : Database<RedisSession>() {
    override fun invoke(statement: RedisSession.() -> Unit) {
        val session = RedisSession(Jedis(host))
        Session.threadLocale.set(session)
        session.statement()
        Session.threadLocale.set(null)
    }
}