package demo

import kotlin.sql.*
import java.util.ArrayList

object Users : Table() {
    val id = varchar("id", length = 10).primaryKey // PKColumn<String>
    val name = varchar("name", length = 50) // Column<String>
    val cityId = integer("city_id", references = Cities.id).nullable // Column<Int?>

    val values = template(id, name, cityId) // Column3<String, String, Int?> Insert template
}

object Cities : Table() {
    val id = integer("id", autoIncrement = true).primaryKey // PKColumn<Int>
    val name = varchar("name", 50) // Column<String>

    val all = template(id, name) // Column2<Int, String> Select template
    val values = template(name) // Column<String> Insert template
}

fun main(args: Array<String>) {
    var db = Database("jdbc:h2:mem:test", driver = "org.h2.Driver")
    // var db = Database("jdbc:mysql://localhost/test", driver = "com.mysql.jdbc.Driver", user = "root")

    db.withSession {
        create (Cities, Users)

        val saintPetersburgId = Cities.insert { values("St. Petersburg")} get Cities.id
        val munichId = Cities.insert {values("Munich")} get Cities.id
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

        println("Select city by name: ")

        Cities.all.filter { name.equals("St. Petersburg") } forEach {
            val (id, name) = it
            println("$id: $name")
        }

        /*
        println("Manual join:")

        select (Users.name, Cities.name) where (Users.id.equals("andrey") or Users.name.equals("Sergey")) and
                Users.id.equals("sergey") and Users.cityId.equals(Cities.id) forEach {
            val (userName, cityName) = it
            println("$userName lives in $cityName")
        }

        println("Join with foreign key:")

        select (Users.name, Users.cityId, Cities.name) from Users join Cities where
                Cities.name.equals("St. Petersburg") or Users.cityId.isNull() forEach {
            val (userName, cityId, cityName) = it
            if (cityId != null) {
                println("$userName lives in $cityName")
            } else {
                println("$userName lives nowhere")
            }
        }

        println("Functions and group by:")

        select (Cities.name, count(Users.id)) from Cities join Users groupBy Cities.name forEach {
            val (cityName, userCount) = it
            if (userCount > 0) {
                println("$userCount user(s) live(s) in $cityName")
            } else {
                println("Nobody lives in $cityName")
            }
        }
        */

        drop (Users, Cities)
    }
}