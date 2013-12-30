package kotlin.nosql.dynamodb

import kotlin.nosql.*
import kotlin.nosql.dynamodb.*
import org.junit.Test

class DynamoDBTests {
    object Users : Table("users") {
        val id = string("id").key() // PKColumn<String, Users>
        val name = string("name") // Column<String, Users>
        val favoriteCityId = nullableInteger("favorite_city_id") // Column<Int?, Users>

        val friendUserIds = setOfString("friend_user_ids") // Column<Set<String>, Users>

        val all = id + name + favoriteCityId + friendUserIds // Template4<Users, String, Int, Int?>
    }

    object Cities : Table("cities") {
        val id = integer("id").key() // PKColumn<Int, Cities>
        val name = string("name") // Column<String, Cities>

        val all = id + name // Template2<Cities, Int, String>
    }

    Test
    fun test() {
        var db = DynamoDB(accessKey = System.getenv("AWS_KEY")!!,secretKey = System.getenv("AWS_SECRET")!!)

        db {
            array(Cities, Users) forEach { it.create() }

            Cities attrs { all } set { values(1, "St. Petersburg") }
            Cities attrs { all } set { values(2, "Munich") }
            Cities attrs { all } set { values(3, "Prague") }

            Users attrs { all } set { values("andrey", "Andrey", 1, setOf("sergey", "eugene")) }
            Users attrs { all } set { values("sergey", "Sergey", 2, setOf("andrey", "eugene", "alex")) }
            Users attrs { all } set { values("eugene", "Eugene", 1, setOf("sergey", "andrey", "alex")) }
            Users attrs { all } set { values("alex", "Alex", 1, setOf("sergey", "eugene", "andrey")) }
            Users attrs { all } set { values("xmth", "Something", null, setOf()) }

            Users attrs { friendUserIds } filter { id eq "andrey" } push { "alex" }

            Users attrs { name } filter { id eq "alex" } set { "Alexey" }

            Users filter { id eq "xmth" } delete { }

            Cities attrs { name } forEach {
                println(it)
            }

            val names = Cities attrs { name } map { it }
            println(names)

            for ((id, name) in Cities attrs { all }) {
                println("$id: $name")
            }

            val cities = Cities attrs { all } map { id, name -> Pair(id, name) }
            println(cities)

            Cities attrs { all } filter { name eq "St. Petersburg" } forEach { id, name ->
                println("$id")
            }

            for ((id, name) in Cities attrs { all } filter { name eq "St. Petersburg" }) {
                println("$id")
            }

            Users attrs { name + favoriteCityId } forEach { userName, cityId ->
                if (cityId != null) {
                    val cityName = Cities attrs { name } get { id eq cityId }
                    println("${userName}'s favorite city is $cityName")
                } else {
                    println("${userName} has no favorite city")
                }
            }

            Users attrs { name + friendUserIds } forEach { userName, friendUserIds ->
                val friends = friendUserIds.map { friendUserId -> Users attrs { name } get { id eq friendUserId } }
                println("${userName}'s friends are: $friends")
            }

            array(Users, Cities) forEach { it.drop() }
        }
    }
}