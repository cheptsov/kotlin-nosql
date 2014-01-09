Kotlin NoSQL Library
==================

A type-safe [Kotlin](https://github.com/JetBrains/kotlin) DSL to access NoSQL databases.

```java
class RedisTests {
    object Users: DocumentSchema<Int, User>("users", javaClass(), integerPK("id")) {
        val Name = string("username")
        val Password = string("password")
        val Posts = listOfInteger("password")
        val Followers = setOfInteger("followers")
        val Following = setOfInteger("following")
        val Auth = nullableString("auth")

        val All = ID + Name + Password
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

    object Posts: TableSchema<Int>("posts", integerPK("id")) {
        val Text = string("text")
    }

    object Global: KeyValueSchema("global") {
        val UserId = integer("nextUserId")
        val PostId = integer("nextPostId")
    }

    Test
    fun test() {
        var db = Redis("localhost")

        db {
            val aUserId = Global next { UserId }
            val anotherUserId = Global next { UserId }

            // insert -> filter + set
            Users columns { All } add { values(aUserId, "antirez", "p1pp0") }
            Users columns { All } add { values(anotherUserId, "pippo", "p1pp0") }

            val aPostId = Global next { PostId }
            val anotherPostId = Global next { PostId }

            Posts columns { ID + Text } add { values(aPostId, "A post") }
            Posts columns { ID + Text } add { values(anotherPostId, "Another post") }

            Users columns { Posts } find { aUserId } add { aPostId }
            Users columns { Posts } find { aUserId } add { anotherPostId }

            Users columns { Followers } find { aUserId } add { anotherUserId }

            Users columns { Name + Password }  find { aUserId } get { name, password ->
                println("User '$name' has password '$password'")
                val posts = Users columns { Posts } find { aUserId } get { 0..20 } map { postId ->
                    Posts columns { Text } get { ID eq postId }
                }
                println("User '$name' has following posts: $posts")
                val followers = Users columns { Followers } get { ID eq aUserId } map { userId ->
                    Users columns { this.Name } get { ID eq userId }
                }
                println("User '$name' has followers $followers")
            }

            val user = Users find { aUserId }
            println("User '${user.name}' has password '$${user.password}'")
            val posts = user.posts map { postId ->
                Posts columns { Text } get { ID eq postId }
            }
            println("User '${user.name}' has following posts: $posts")
            val followers = user.followers map { userId ->
                Users columns { this.Name } get { ID eq userId }
            }
            println("User '${user.name}' has followers $followers")

            //Users add { User(Global next { UserId }, "andrey.cheptsov", "pass2013") }

            for (user in Users filter { ID eq aUserId }) {
                println(user)
            }
        }
    }
```