package kotlin.nosql.dynamodb

import kotlin.nosql.*
import kotlin.nosql.dynamodb.*
import org.junit.Test

class DynamoDBTests {
    object Users : Schema("users") {
        val id = stringPK("id") // PkColumn<String, Users>
        val name = string("name") // Column<String, Users>
        val favoriteCityId = nullableInteger("favorite_city_id") // Column<Int?, Users>

        val friendUserIds = setOfString("friend_user_ids") // Column<Set<String>, Users>

        val all = id + name + favoriteCityId + friendUserIds // Template4<Users, String, Int, Int?>
    }

    object Cities : Schema("cities") {
        val id = integer("id").primaryKey() // PKColumn<Int, Cities>
        val name = string("name") // Column<String, Cities>

        val all = id + name // Template2<Cities, Int, String>
    }

    Test
    fun test() {
        var db = DynamoDB(accessKey = System.getenv("AWS_KEY")!!, secretKey = System.getenv("AWS_SECRET")!!)

        db {
            array(Cities, Users) forEach { it.create() }

            Cities columns { all } insert { values(1, "St. Petersburg") }
            Cities columns { all } insert { values(2, "Munich") }
            Cities columns { all } insert { values(3, "Prague") }

            Users columns { all } insert { values("andrey", "Andrey", 1, setOf("sergey", "eugene")) }
            Users columns { all } insert { values("sergey", "Sergey", 2, setOf("andrey", "eugene", "alex")) }
            Users columns { all } insert { values("eugene", "Eugene", 1, setOf("sergey", "andrey", "alex")) }
            Users columns { all } insert { values("alex", "Alex", 1, setOf("sergey", "eugene", "andrey")) }
            Users columns { all } insert { values("xmth", "Something", null, setOf()) }

            Users columns { friendUserIds } filter { id eq "andrey" } add { "alex" }

            Users columns { name } filter { id eq "alex" } set { "Alexey" }

            Users delete { id eq "xmth" }

            Cities columns { name } forEach {
                println(it)
            }

            val names = Cities columns { name } map { it }
            println(names)

            for ((id, name) in Cities columns { all }) {
                println("$id: $name")
            }

            val cities = Cities columns { all } map { id, name -> Pair(id, name) }
            println(cities)

            Cities columns { all } filter { name eq "St. Petersburg" } forEach { id, name ->
                println("$id")
            }

            for ((id, name) in Cities columns { all } filter { name eq "St. Petersburg" }) {
                println("$id")
            }

            Users columns { name + favoriteCityId } forEach { userName, cityId ->
                if (cityId != null) {
                    val cityName = Cities columns { name } get { id eq cityId }
                    println("${userName}'s favorite city is $cityName")
                } else {
                    println("${userName} has no favorite city")
                }
            }

            Users columns { name + friendUserIds } forEach { userName, friendUserIds ->
                val friends = friendUserIds.map { friendUserId -> Users columns { name } get { id eq friendUserId } }
                println("${userName}'s friends are: $friends")
            }

            array(Users, Cities) forEach { it.drop() }
        }
    }
}