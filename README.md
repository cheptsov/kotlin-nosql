Kotlin SQL Library
==================

_Exposed_ is a prototype for a lightweight SQL library written over JDBC driver for [Kotlin](https://github.com/JetBrains/kotlin) language.

```java
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
        create (Cities, Users)

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

        println("Select city by name: ")

        Cities.all.filter { name.equals("St. Petersburg") } forEach {
            val (id, name) = it
            println("$id: $name")
        }

        println("Select from two tables: ")

        (Cities.name * Users.name).filter { Users.cityId.equals(Cities.id) } forEach {
            val (cityName, userName) = it
            println("$userName lives in $cityName")
        }

        drop (Users, Cities)
    }
}
```

Outputs:

    SQL: CREATE TABLE Cities (id INT PRIMARY KEY AUTO_INCREMENT NOT NULL, name VARCHAR(50) NOT NULL)
    SQL: CREATE TABLE Users (id VARCHAR(10) PRIMARY KEY NOT NULL, name VARCHAR(50) NOT NULL, city_id INT NULL)
    SQL: INSERT INTO Cities (name) VALUES ('St. Petersburg')
    SQL: INSERT INTO Cities (name) VALUES ('Munich')
    SQL: INSERT INTO Cities (name) VALUES ('Prague')
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('andrey', 'Andrey', 1)
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('sergey', 'Sergey', 2)
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('eugene', 'Eugene', 2)
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('alex', 'Alex', null)
    SQL: INSERT INTO Users (id, name, city_id) VALUES ('smth', 'Something', null)
    SQL: UPDATE Users SET name = 'Alexey' WHERE Users.id = 'alex'
    SQL: DELETE FROM Users WHERE Users.name LIKE '%thing'
    All cities:
    SQL: SELECT Cities.id, Cities.name FROM Cities
    1: St. Petersburg
    2: Munich
    3: Prague
    Select city by name:
    SQL: SELECT Cities.id, Cities.name FROM Cities WHERE Cities.name = 'St. Petersburg'
    1: St. Petersburg
    Select from two tables:
    SQL: SELECT Cities.name, Users.name FROM Users, Cities WHERE Users.city_id = Cities.id
    Andrey lives in St. Petersburg
    Sergey lives in Munich
    Eugene lives in Munich
    SQL: DROP TABLE Users
    SQL: DROP TABLE Cities