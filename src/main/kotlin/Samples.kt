package demo

import kotlin.sql.*

object Users : Table() {
    val id = varchar("id", length = 10).id() // PKColumn<String, Users>
    val name = varchar("name", length = 50) // Column<String, Users>
    val requiredCityId = integer("required_city_id").references(Cities.id) // FKColumn<Int, Users>
    val optionalCityId = integer("optional_city_id").references(Cities.id).optional() // FKOptionColumn<Int, Users>

    val all = id + name + requiredCityId + optionalCityId // Template4<Users, String, Int, Int?> Select template
    val values = id + name + requiredCityId + optionalCityId // Template4<Users, String, Int, Int?> Insert template
}

object Cities : Table() {
    val id = integer("id").id().generated() // GeneratedPKColumn<Int, Cities>
    val name = varchar("name", 50) // Column<String, Cities>

    val all = id + name // Template2<Cities, Int, String> Select template
    val values = name // Column<String, Cities>
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

        Users.filter { id.eq("alex") } update {
            it[name] = "Alexey"
        }

        Users.delete { name.like("%thing") }

        Cities select { name } forEach {
            println(it)
        }

        for ((id, name) in Cities select { all }) {
            println("$id: $name")
        }

        Cities select { all } filter { name.eq("St. Petersburg") } forEach { id, name ->
            println("$id: $name")
        }

        for ((id, name) in Cities select { all } filter { name.eq("St. Petersburg") }) {
            println("$id: $name")
        }

        Users select { name + Users.requiredCityId } forEach { userName, userRequiredCityId ->
            val cityName = Cities select {name } find { id.eq(userRequiredCityId) }
            println("$userName's required city is $cityName")
        }

        array(Users, Cities).forEach { it.drop() }
    }
}