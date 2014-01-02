package kotlin.nosql.redis

import org.junit.Test
import kotlin.nosql.*

class RedisTests {
    object Users: Schema("users") {
        val id = integerPK("id") // PKColumn<Int, Users>
        val name = string("username") // Column<String, Users>
        val password = string("password") // Column<String, Users>
        val posts = listOfInteger("posts") // Column<List<Int>>, Users>
        val followers = setOfInteger("followers") // Column<Set<Int>>, Users>
        val following = setOfInteger("following") // Column<Set<Int>>, Users>
        val auth = nullableString("auth") // Column<String?>, Users>

        val all = id + name + password // Template3<Users, Int, String, String>
    }

    object Posts: Schema("posts") {
        val id = integerPK("id") // PKColumn<Int, Posts>
        val text = string("text") // Column<String, Posts>
    }

    object Global: Schema("global") {
        val userId = integer("nextUserId") // Column<Int, Global>
        val postId = integer("nextPostId") // Column<Int, Global>
    }

    Test
    fun test() {
        var db = Redis("localhost")

        db {
            val aUserId = Global next { userId }
            val anotherUserId = Global next { userId }

            Users columns { all } insert { values(aUserId, "antirez", "p1pp0") }
            Users columns { all } insert { values(anotherUserId, "pippo", "p1pp0") }

            val aPostId = Global next { postId }
            val anotherPostId = Global next { postId }

            Posts columns { id + text } insert { values(aPostId, "A post") }
            Posts columns { id + text } insert { values(anotherPostId, "Another post") }

            Users columns { posts } filter { id eq aUserId } add { aPostId }
            Users columns { posts } filter { id eq aUserId } add { anotherPostId }

            Users columns { followers } filter { id eq aUserId } add { anotherUserId }

            Users columns { name + password } filter { id eq aUserId } get { name, password ->
                println("User '$name' has password '$password'")
                val posts = Users columns { posts } filter { id eq aUserId } get { 0..20 } map { postId ->
                    Posts columns { text } get { id eq postId }
                }
                println("User '$name' has following posts: $posts")
                val followerIds: Set<Int> = Users columns { followers } get { id eq aUserId }
                val followers = followerIds map { userId ->
                    Users columns { this.name } get { id eq userId }
                }
                println("User '$name' has followers $followers")
            }

            array(Posts, Users).forEach { it.drop() }
        }
    }
}

