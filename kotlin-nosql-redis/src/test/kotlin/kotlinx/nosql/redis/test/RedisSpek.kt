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

    init {
        given("a key value schema") {
            val redis = Redis(schemas = array(Global, Users, Posts), action = CreateDrop())

            redis.withSession {
                val userId = Global.nextUserId.incr()
                val postId = Global.nextPostId.incr()

                assertEquals(1, userId.value)
                assertEquals(1, postId.value)

                val (u, p) = Global.projection { nextUserId + nextPostId }.single()

                assertEquals(1, u.value)
                assertEquals(1, p.value)

                Posts.insert(Post(postId, "Test"))

                Users.insert(User(userId, "andrey.cheptsov", "123", listOf(postId)))

                val user = Users.find(userId).get()

                assertEquals(userId, user.id)
                assertEquals("andrey.cheptsov", user.name)
                assertEquals("123", user.password)
                assertEquals(1, user.posts.size)
                assertEquals(postId, user.posts.first)

                val anotherPostId = Global.nextPostId.incr()
                Posts.insert(Post(anotherPostId, "Test (another post)"))

                Users.find(userId).projection { posts }.add(anotherPostId)

                val posts = Users.find(userId).projection { posts }.single().map { Posts.find(it).single() }

                assertEquals(1, posts.size)
                assertEquals(postId, posts[1].id)
                assertEquals("Test", posts[1].body)
                assertEquals(anotherPostId, posts[2].id)
                assertEquals("Test (another post)", posts[2].body)

                Users.find(userId).projection { auth }.update("x")

                val auth = Users.find(userId).projection { auth }.single()
                assertEquals("x", auth!!)
            }
        }
    }
}