package kotlinx.nosql.redis.test

import kotlinx.nosql.*
import kotlinx.nosql.redis.*
import org.jetbrains.spek.api.Spek
import kotlin.test.assertNotNull
import kotlin.test.assertEquals

class RedisSpek : Spek() {
    object Global: KeyValueSchema("global") {
        val nextUserId = id("next_user id", Users)
        val nextPostId = id("next_post_id", Posts)
    }

    object Users: DocumentSchema<Int, User>("users", javaClass(), integer("id")) {
        val name = string("username")
        val password = string("password")
        val posts = listOfId("posts", Posts)
        val followers = setOfInteger("followers")
        val following = setOfInteger("following")
        val auth = nullableString("auth")
    }

    data class User(val id: Id<Int, Users>,
               val name: String,
               val password: String,
               val posts: List<Id<Int, Posts>> = listOf(),
               val followers: Set<Id<Int, Users>> = setOf(),
               val following: Set<Id<Int, Users>> = setOf(),
               val auth: String? = null) {
    }

    object Posts: DocumentSchema<Int, Post>("posts", javaClass(), integer("id")) {
        val body = string("body")
    }

    data class Post(val id: Id<Int, Posts>, val body: String) {
    }

    {
        given("a key value schema") {
            val redis = Redis(schemas = array(Global, Users, Posts), action = CreateDrop())

            redis.withSession {
                val userId = Global.nextUserId.incr()
                val postId = Global.nextPostId.incr()
                //val (u, p) = Global.projection { nextUserId + nextPostId }.get()

                Posts.insert(Post(postId, "Test"))

                Users.insert(User(userId, "andrey.cheptsov", "123", listOf(postId)))
                // Users.find(id).projection { posts }.add(postId)

                //Users.find(userId).projection { auth }.update("x")
                // Users.find(userId).projection { auth }.get()

                val user = Users.find(userId).get()
                // val posts: List<Id<Int, Posts>> = Users.find(userId).projection { posts }.get()
                assertEquals(userId, user.id)
                assertEquals("andrey.cheptsov", user.name)
                assertEquals("123", user.password)
                assertEquals(1, user.posts.size)
                assertEquals(postId, user.posts.first)
            }
        }
    }
}
