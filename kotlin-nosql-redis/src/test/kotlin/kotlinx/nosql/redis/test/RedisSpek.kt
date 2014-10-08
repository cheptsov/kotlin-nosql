package kotlinx.nosql.redis.test

import kotlinx.nosql.*
import kotlinx.nosql.redis.*
import org.jetbrains.spek.api.Spek

class RedisSpek : Spek() {
    object Global: KeyValueSchema("global") {
        val userId = id("next_user id", Users)
        val postId = id("next_post_id", Posts)
    }

    object Users: DocumentSchema<Int, User>("users", javaClass(), integer("id")) {
        val name = string("username")
        val password = string("password")
        val posts = listOfId("posts", Posts)
        val followers = setOfInteger("followers")
        val following = setOfInteger("following")
        val auth = nullableString("auth")
    }

    class User(val id: Id<Int, Users>,
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

    fun <String, B : Id<*, *>> String.myfunc(st: () -> Id<B, *>) {

    }

    {
        given("a key value schema") {
            val redis = Redis(schemas = array(Global, Users, Posts), action = CreateDrop())

            redis.withSession {
                val userId = Global.next { userId }

                Users.insert(User(userId, "andrey.cheptsov", "123"))
            }
        }
    }
}
