package kotlin.nosql.redis

import org.junit.Test
import kotlin.nosql.*

class RedisTests {
    object Users: DocumentSchema<Int, User>("users", javaClass(), primaryKey = integer("id")) {
        val Name = string("username")
        val Password = string("password")
        val Posts = listOfInteger("password")
        val Followers = setOfInteger("followers")
        val Following = setOfInteger("following")
        val Auth = nullableString("auth")

        val All = Name + Password
    }

    class User(val id: Int,
               val name: String,
               val password: String,
               val posts: List<Int> = listOf(),
               val followers: Set<Int> = setOf(),
               val following: Set<Int> = setOf(),
               val auth: String? = null) {

        fun toString(): String {
            return "User(id = $id, name = $name, password = $password, posts = $posts, followers = $followers, following = $following, auth = $auth)"
        }
    }

    object Posts: PKTableSchema<Int>("posts", PK.integer("id")) {
        val Text = string("text")
    }

    object Global: KeyValueSchema("global") {
        val UserId = integer("nextUserId")
        val PostId = integer("nextPostId")
    }

    Test
    fun test() {
        var db = Redis("localhost", array<AbstractSchema>(Users, Posts)) // Compiler failure

        db {
            val aUserId = Global next { UserId }
            val anotherUserId = Global next { UserId }

            Users columns { All } insert { values(aUserId, "antirez", "p1pp0") }
            Users columns { All } insert { values(anotherUserId, "pippo", "p1pp0") }

            val aPostId = Global next { PostId }
            val anotherPostId = Global next { PostId }

            Posts columns { Text } insert { values(aPostId, "A post") }
            Posts columns { Text } insert { values(anotherPostId, "Another post") }

            Users columns { Posts } find { aUserId } add { aPostId }
            Users columns { Posts } find { aUserId } add { anotherPostId }

            Users columns { Followers } find { aUserId } add { anotherUserId }

            val (name, password) = Users columns { Name + Password } get { aUserId }

            println("User '$name' has password '$password'")
            val posts = Users columns { Posts } find { aUserId } get { 0..20 } map { postId ->
                Posts columns { Text } get { postId }
            }
            println("User '$name' has following posts: $posts")
            val followers = Users columns { Followers } get { aUserId } map { userId ->
                Users columns { this.Name } get { userId }
            }
            println("User '$name' has followers $followers")

            val oneMoreUserId = Global next { UserId }
            Users insert { User(oneMoreUserId, "andrey.cheptsov", "pass2013") }

            val oneMoreUser = Users get { oneMoreUserId }
            println("User '${oneMoreUser.name}' has password '$${oneMoreUser.password}'")
            val oneMoreUserPosts = oneMoreUser.posts map { postId ->
                Posts columns { Text } get { postId }
            }
            println("User '${oneMoreUser.name}' has following posts: $oneMoreUserPosts")
            val oneMoreUserFollowers = oneMoreUser.followers map { userId ->
                Users columns { this.Name } get { userId }
            }
            println("User '${oneMoreUser.name}' has followers $oneMoreUserFollowers")

            for (user in Users filter { ID eq oneMoreUserId }) {
                println(user)
            }
        }
    }
}

