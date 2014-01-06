Kotlin NoSQL Library
==================

A type-safe [Kotlin](https://github.com/JetBrains/kotlin) DSL to access NoSQL databases.

```java
class RedisTests {
    object Users: DocumentSchema<User>("users", javaClass()) {
        val ID = Column("id", javaClass<Int>()).PrimaryKey()
        val Name = Column("username", javaClass<String>())
        val Password = Column("password", javaClass<String>())
        val Posts = Column("password", javaClass<Int>()).List()
        val Followers = Column("followers", javaClass<Int>()).Set()
        val Following = Column("following", javaClass<Int>()).Set()
        val Auth = Column("auth", javaClass<String>()).Nullable()

        val All = ID + Name + Password
    }

    class User(val id: Int,
               val name: String,
               val password: String,
               val posts: List<Int>,
               val followers: Set<Int>,
               val following: Set<Int>,
               val auth: String?) {

        fun toString(): String {
            return "User(id = $id, name = $name, password = $password, posts = $posts, followers = $followers, following = $following, auth = $auth)"
        }
    }

    object Posts: TableSchema("posts") {
        val ID = Column("id", javaClass<Int>()).PrimaryKey()
        val Text = Column("text", javaClass<String>())
    }

    object Global: KeyValueSchema("global") {
        val UserId = Column("nextUserId", javaClass<Int>())
        val PostId = Column("nextPostId", javaClass<Int>())
    }

    Test
    fun test() {
        var db = Redis("localhost")

        db {
            val aUserId = Global next { UserId }
            val anotherUserId = Global next { UserId }

            Users columns { All } insert { values(aUserId, "antirez", "p1pp0") }
            Users columns { All } insert { values(anotherUserId, "pippo", "p1pp0") }

            val aPostId = Global next { PostId }
            val anotherPostId = Global next { PostId }

            Posts columns { ID + Text } insert { values(aPostId, "A post") }
            Posts columns { ID + Text } insert { values(anotherPostId, "Another post") }

            Users columns { Posts } filter { ID eq aUserId } add { aPostId }
            Users columns { Posts } filter { ID eq aUserId } add { anotherPostId }

            Users columns { Followers } filter { ID eq aUserId } add { anotherUserId }

            Users columns { Name + Password } filter { ID eq aUserId } get { name, password ->
                println("User '$name' has password '$password'")
                val posts = Users columns { Posts } filter { ID eq aUserId } get { 0..20 } map { postId ->
                    Posts columns { Text } get { ID eq postId }
                }
                println("User '$name' has following posts: $posts")
                val followers = Users columns { Followers } get { ID eq aUserId } map { userId ->
                    Users columns { this.Name } get { ID eq userId }
                }
                println("User '$name' has followers $followers")
            }

            Users filter { ID eq aUserId } map {
                println()
            }

            val user = Users get { ID eq aUserId }
            println(user.toString())
        }
    }
}
```