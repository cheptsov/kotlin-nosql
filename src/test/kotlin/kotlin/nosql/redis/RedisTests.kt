package kotlin.nosql.redis

import org.junit.Test
import kotlin.nosql.*
import kotlin.nosql.dynamodb.DynamoDB

class RedisTests {
    object Users: Table("users") {
        val id = integer("id").key()
        val username = string("username")
        val password = string("password")
        val posts = setOfString("posts")
        val followers = setOfInteger("followers")
        val following = setOfInteger("following")
        val auth = nullableString("auth")
    }

    object Posts: Table("posts") {
        val id = integer("id").key()
        val text = string("text")
    }

    Test
    fun test() {
        var db = DynamoDB(System.getenv("AWS_KEY")!!, System.getenv("AWS_SECRET")!!)

        db {
            // Add new post

            Posts attrs { id + text } set { values(1, "New post") }

            // Update existing post

            Posts attrs { text } filter { id eq 1 } set { "" }

            // Delete existing post

            Posts filter { id eq 1 } delete { }

            // Get post's text by its id

            Posts attrs { text } get { id eq 1 }

            val (id, followers) = (Users attrs { id + followers } get { id eq 1 })!!

            followers.forEach {  }
        }
    }
}

