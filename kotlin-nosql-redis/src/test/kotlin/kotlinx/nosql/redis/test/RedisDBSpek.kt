package kotlinx.nosql.redis.test

import kotlinx.nosql.*
import kotlinx.nosql.redis.*
import org.jetbrains.spek.api.Spek

class RedisDBSpek : Spek() {
    object Global: KeyValueSchema("global") {
        val nextUserId = integer("nextUserId")
        val nextPostId = integer("nextPostId")
    }

    object Users: DocumentSchema<Int, User>("users", javaClass(), integer("id")) {
        val name = string("username")
        val password = string("password")
        val posts = listOfId("posts", Posts)
        val followers = setOfInteger("followers")
        val following = setOfInteger("following")
        val auth = nullableString("auth")
    }

    class User(val id: Int? = null,
               val name: String,
               val password: String,
               val posts: List<Int> = listOf(),
               val followers: Set<Int> = setOf(),
               val following: Set<Int> = setOf(),
               val auth: String? = null) {
    }

    object Posts: DocumentSchema<Int, Post>("posts", javaClass(), integer("id")) {
        val body = string("body")
    }

    class Post(val id: Int? = null, val body: String) {
    }

    {
        given("a key value schema") {
            val db = RedisDB(schemas = array(Global, Users, Posts))

            db.withSession {
                Users.insert(User(1, "andrey.cheptsov", "123"))
            }
        }
    }
}
