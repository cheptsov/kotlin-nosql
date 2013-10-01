package demo

import kotlin.sql.*

object Users : Table() {
    val id = varchar("id", length = 10).id() // PKColumn<String, Users>
    val name = varchar("name", length = 50) // Column<String, Users>
    val requiredCityId = integer("required_city_id").references(Cities.id) // FKColumn<Int, Users>
    val optionalCityId = integer("optional_city_id").references(Cities.id).optional() // FKOptionColumn<Int, Users>

    val all = template(id, name, requiredCityId, optionalCityId) // Template4<Users, String, String, Int?> Select template
    val values = template(id, name, requiredCityId, optionalCityId) // Template4<Users, String, String, Int?> Insert template
}

object Cities : Table() {
    val id = integer("id").id().generated() // GeneratedPKColumn<Int, Cities>
    val name = varchar("name", 50) // Column<String, Cities>

    val all = template(id, name) // Template2<Cities, Int, String> Select template
    val values = template(name) // Template<Cities, String> Insert template
}

fun main(args: Array<String>) {
    var db = Database("jdbc:h2:mem:test", driver = "org.h2.Driver")
    // var db = Database("jdbc:mysql://localhost/test", driver = "com.mysql.jdbc.Driver", user = "root")

    db.withSession {
        array(Cities, Users).forEach { it.create() }

        val saintPetersburgId = Cities.insert { values("St. Petersburg") } get { id }
        val munichId = Cities.insert { values("Munich") } get { id }
        Cities.insert { values("Prague") }

        Users.insert { values("andrey", "Andrey", saintPetersburgId, saintPetersburgId) }
        Users.insert { values("sergey", "Sergey", munichId, munichId) }
        Users.insert { values("eugene", "Eugene", munichId, null) }
        Users.insert { values("alex", "Alex", munichId, null) }
        Users.insert { values("smth", "Something", munichId, null) }

        Users.filter { id.equals("alex") } update {
            it[name] = "Alexey"
        }

        Users.delete { name.like("%thing") }

        println("All cities:")

        Cities.all().forEach {
            val (id, name) = it
            println("$id: $name")
        }

        println("Select city by name:")

        Cities.all.filter { name.equals("St. Petersburg") } forEach {
            val (id, name) = it // Int, String
            println("$id: $name")
        }

        println("Select from two tables:")

        (Cities.name * Users.name).filter { Users.optionalCityId.equals(Cities.id) } forEach {
            val (cityName, userName) = it // String, String
            println("$userName lives in $cityName")
        }

        println("Inner join: ")

        (Users.name + Users.requiredCityId * Cities.name) forEach {
            val (userName, cityName) = it // String, String
            println("$userName's required city is $cityName")
        }

        println("Left join: ")

        (Users.id + Users.name + Users.optionalCityId * Cities.all).forEach {
            val (userId, userName, cityId, cityName) = it  // String, String, Int?, String?
            if (cityName != null) {
                println("$userName's optional city is $cityName")
            } else {
                println("$userName has no optional city")
            }
        }

        array(Users, Cities).forEach { it.drop() }
    }
}