package kotlin.nosql.examples

import kotlin.nosql.*
import kotlin.nosql.dynamodb.*

object Users : Table() {
    val id = string("id", length = 10).id() // PKColumn<String, Users>
    val name = string("name", length = 50) // Column<String, Users>
    val requiredCityId = integer("required_city_id") // Column<Int, Users>
    val optionalCityId = integer("optional_city_id").nullable() // Column<Int?, Users>

    val all = id + name + requiredCityId + optionalCityId // Template4<Users, String, Int, Int?>
}

object Cities : Table() {
    val id = integer("id").id() // PKColumn<Int, Cities>
    val name = string("name", 50) // Column<String, Cities>

    val all = id + name // Template2<Cities, Int, String>
}

fun main(args: Array<String>) {
    var db = DynamoDB()

    db {
        array(Cities, Users) forEach { it.create() }

        Cities columns { all } insert { values(1, "St. Petersburg") }
        Cities columns { all } insert { values(2, "Munich") }
        Cities columns { all } insert { values(3, "Prague") }

        Users columns { all } insert { values("andrey", "Andrey", 1, 1) }
        Users columns { all } insert { values("sergey", "Sergey", 2, 2) }
        Users columns { all } insert { values("eugene", "Eugene", 2, null) }
        Users columns { all } insert { values("alex", "Alex", 2, null) }
        Users columns { all } insert { values("xmth", "Something", 2, 1) }

        Users update { id eq "alex" } set {
            it[name] = "Alexey"
        }

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

        Users columns { name + requiredCityId } forEach { userName, requiredCityId ->
            val cityName = Cities columns { name } find { id eq requiredCityId }
            println("${userName}'s required city is $cityName")
        }

        array(Users, Cities) forEach { it.drop() }
    }
}