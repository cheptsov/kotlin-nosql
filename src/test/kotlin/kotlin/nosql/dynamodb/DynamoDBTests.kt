package kotlin.nosql.dynamodb

import kotlin.nosql.*
import kotlin.nosql.dynamodb.*
import org.junit.Test

class DynamoDBTests {
    object Users : TableSchema<String>("users", string("id")) {
        val Name = string("name")
        val FavoriteCityId = nullableInteger("favorite_city_id")

        val FriendUserIds = setOfString("friend_user_ids")

        val All = ID + Name + FavoriteCityId + FriendUserIds
    }

    object Cities : TableSchema<Int>("cities", integer("id")) {
        val Name = string("name")

        val All = ID + Name
    }

    Test
    fun test() {
        var db = DynamoDB(accessKey = System.getenv("AWS_KEY")!!, secretKey = System.getenv("AWS_SECRET")!!,
                schemas = array<Schema>(Users, Cities)) // Compiler failure

        db {
            array(Cities, Users) forEach { it.create() }

            Cities columns { All } put { values(1, "St. Petersburg") }
            Cities columns { All } put { values(2, "Munich") }
            Cities columns { All } put { values(3, "Prague") }

            Users columns { All } insert { values("andrey", "Andrey", 1, setOf("sergey", "eugene")) }
            Users columns { All } insert { values("sergey", "Sergey", 2, setOf("andrey", "eugene", "alex")) }
            Users columns { All } insert { values("eugene", "Eugene", 1, setOf("sergey", "andrey", "alex")) }
            Users columns { All } insert { values("alex", "Alex", 1, setOf("sergey", "eugene", "andrey")) }
            Users columns { All } insert { values("xmth", "Something", null, setOf()) }

            Users columns { FriendUserIds } filter { ID eq "andrey" } add { "alex" }

            Users columns { Name } filter { ID eq "alex" } set { "Alexey" }

            Users delete { ID eq "xmth" }

            Cities columns { Name } forEach {
                println(it)
            }

            val names = Cities columns { Name } map { it }
            println(names)

            for ((id, name) in Cities columns { All }) {
                println("$id: $name")
            }

            val cities = Cities columns { All } map { id, name -> Pair(id, name) }
            println(cities)

            Cities columns { All } filter { Name eq "St. Petersburg" } forEach { id, name ->
                println("$id")
            }

            for ((id, name) in Cities columns { All } filter { Name eq "St. Petersburg" }) {
                println("$id")
            }

            Users columns { Name + FavoriteCityId } forEach { userName, cityId ->
                if (cityId != null) {
                    val cityName = Cities columns { Name } get { cityId!! } // Type inference failure
                    println("${userName}'s favorite city is $cityName")
                } else {
                    println("${userName} has no favorite city")
                }
            }

            Users columns { Name + FriendUserIds } forEach { userName, friendUserIds ->
                val friends = friendUserIds.map { friendUserId -> Users columns { Name } get { friendUserId } }
                println("${userName}'s friends are: $friends")
            }

            array(Users, Cities) forEach { it.drop() }
        }
    }
}