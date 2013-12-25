Kotlin NoSQL Library
==================

_Exposed_ is a prototype for a NoSQL (SQL without JOINs) library for [Kotlin](https://github.com/JetBrains/kotlin) language.

```java
import kotlin.nosql.*

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
        array(Cities, Users) forEach { it.create() }

        val saintPetersburgId = Cities insert { values("St. Petersburg") } get { id }
        val munichId = Cities insert { values("Munich") } get { id }
        Cities insert { values("Prague") }

        Users insert { values("andrey", "Andrey", saintPetersburgId, saintPetersburgId) }
        Users insert { values("sergey", "Sergey", munichId, munichId) }
        Users insert { values("eugene", "Eugene", munichId, null) }
        Users insert { values("alex", "Alex", munichId, null) }
        Users insert { values("smth", "Something", munichId, null) }

        Users update { id eq "alex" } set {
            it[name] = "Alexey"
        }

        Users delete { name like "%thing" }

        Cities select { name } forEach {
            println(it)
        }

        for ((id, name) in Cities select { all }) {
            println("$id: $name")
        }

        Cities select { all } filter { name.eq("St. Petersburg") } forEach { id, name ->
            println("$id: $name")
        }

        for ((id, name) in Cities select { all } filter { name eq "St. Petersburg" }) {
            println("$id: $name")
        }

        Users select { name + Users.requiredCityId } forEach { userName, userRequiredCityId ->
            val cityName = Cities select {name } find { id eq userRequiredCityId }
            println("$userName's required city is $cityName")
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