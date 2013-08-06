package demo

import kotlin.sql.*
import java.util.ArrayList

object Users : Table() {
    val id = varchar("id", length = 10).primaryKey() // PKColumn<String, Users>
    val name = varchar("name", length = 50) // Column<String, Users>
    val cityId = integer("city_id").foreignKey(Cities.id).nullable() // FKColumn<Int?, Users>

    val all = template(id, name, cityId) // Template3<Users, String, String, Int?> Select template
    val values = template(id, name, cityId) // Template3<Users, String, String, Int?> Insert template
}

object Cities : Table() {
    val id = integer("id").primaryKey().auto() // GeneratedPKColumn<Int, Cities>
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

        Users.insert { values("andrey", "Andrey", saintPetersburgId) }
        Users.insert { values("sergey", "Sergey", munichId) }
        Users.insert { values("eugene", "Eugene", munichId) }
        Users.insert { values("alex", "Alex", null) }
        Users.insert { values("smth", "Something", null) }

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
            val (id, name) = it
            println("$id: $name")
        }

        println("Select from two tables:")

        (Cities.name * Users.name).filter { Users.cityId.equals(Cities.id) } forEach {
            val (cityName, userName) = it
            println("$userName lives in $cityName")
        }

        (Users.id + Users.name + Users.cityId * Cities.all).forEach {
            val (userId, userName, userCityId, cityId, cityName) = it
            if (userCityId != null) {
                println("$userName lives in $cityName")
            } else {
                println("$userName lives nowhere")
            }
        }

        array(Users, Cities).forEach { it.drop() }
    }
}