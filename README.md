Kotlin NoSQL Library
==================

A type-safe [Kotlin](https://github.com/JetBrains/kotlin) DSL for accessing NoSQL database.

```java
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
```

Outputs:

    SQL: CREATE TABLE Cities (id INT PRIMARY KEY AUTO_INCREMENT NOT NULL, name VARCHAR(50) NOT NULL)
    SQL: CREATE TABLE Users (id VARCHAR(10) PRIMARY KEY NOT NULL, name VARCHAR(50) NOT NULL, required_city_id INT NOT NULL, optional_city_id INT NULL)
    SQL: ALTER TABLE Users ADD CONSTRAINT required_city_id FOREIGN KEY (required_city_id) REFERENCES Cities(id)
    SQL: ALTER TABLE Users ADD CONSTRAINT optional_city_id FOREIGN KEY (optional_city_id) REFERENCES Cities(id)
    SQL: INSERT INTO Cities (name) VALUES ('St. Petersburg')
    SQL: INSERT INTO Cities (name) VALUES ('Munich')
    SQL: INSERT INTO Cities (name) VALUES ('Prague')
    SQL: INSERT INTO Users (id, name, required_city_id, optional_city_id) VALUES ('andrey', 'Andrey', 1, 1)
    SQL: INSERT INTO Users (id, name, required_city_id, optional_city_id) VALUES ('sergey', 'Sergey', 2, 2)
    SQL: INSERT INTO Users (id, name, required_city_id, optional_city_id) VALUES ('eugene', 'Eugene', 2, null)
    SQL: INSERT INTO Users (id, name, required_city_id, optional_city_id) VALUES ('alex', 'Alex', 2, null)
    SQL: INSERT INTO Users (id, name, required_city_id, optional_city_id) VALUES ('smth', 'Something', 2, null)
    SQL: UPDATE Users SET name = 'Alexey' WHERE Users.id = 'alex'
    SQL: DELETE FROM Users WHERE Users.name LIKE '%thing'
    All cities via forEach:
    SQL: SELECT Cities.name FROM Cities
    St. Petersburg
    Munich
    Prague
    All cities via for:
    SQL: SELECT Cities.id, Cities.name FROM Cities
    1: St. Petersburg
    2: Munich
    3: Prague
    Select city by name via forEach:
    SQL: SELECT Cities.id, Cities.name FROM Cities WHERE Cities.name = 'St. Petersburg'
    1: St. Petersburg
    Select city by name via forEach:
    SQL: SELECT Cities.id, Cities.name FROM Cities WHERE Cities.name = 'St. Petersburg'
    1: St. Petersburg
    SQL: SELECT Users.name, Users.required_city_id FROM Users
    SQL: SELECT Cities.name FROM Cities WHERE Cities.id = 1
    Andrey's required city is St. Petersburg
    SQL: SELECT Cities.name FROM Cities WHERE Cities.id = 2
    Sergey's required city is Munich
    SQL: SELECT Cities.name FROM Cities WHERE Cities.id = 2
    Eugene's required city is Munich
    SQL: SELECT Cities.name FROM Cities WHERE Cities.id = 2
    Alexey's required city is Munich
    SQL: DROP TABLE Users
    SQL: DROP TABLE Cities