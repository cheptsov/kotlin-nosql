Kotlin SQL Library
==================

_Exposed_ is a prototype for a lightweight SQL library written over JDBC driver for [Kotlin](https://github.com/JetBrains/kotlin) language.

```java
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
```

Outputs:

    SQL: CREATE TABLE Cities (id INT PRIMARY KEY AUTO_INCREMENT NOT NULL, name VARCHAR(50) NOT NULL)
    SQL: CREATE TABLE Users (id VARCHAR(10) PRIMARY KEY NOT NULL, name VARCHAR(50) NOT NULL, required_city_id INT NULL, optional_city_id INT NULL)
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
    All cities:
    SQL: SELECT Cities.id, Cities.name FROM Cities
    1: St. Petersburg
    2: Munich
    3: Prague
    Select city by name:
    SQL: SELECT Cities.id, Cities.name FROM Cities WHERE Cities.name = 'St. Petersburg'
    1: St. Petersburg
    Select from two tables:
    SQL: SELECT Cities.name, Users.name FROM Cities, Users WHERE Users.optional_city_id = Cities.id
    Andrey lives in St. Petersburg
    Sergey lives in Munich
    Left join:
    SQL: SELECT Users.name, Cities.name FROM Users INNER JOIN Cities ON Users.required_city_id = Cities.id
    Andrey's required city is St. Petersburg
    Sergey's required city is Munich
    Eugene's required city is Munich
    Alexey's required city is Munich
    Inner join:
    SQL: SELECT Users.id, Users.name, Cities.id, Cities.name FROM Users LEFT JOIN Cities ON Users.optional_city_id = Cities.id
    Andrey's optional city is St. Petersburg
    Sergey's optional city is Munich
    Eugene has no optional city
    Alexey has no optional city
    SQL: DROP TABLE Users
    SQL: DROP TABLE Cities
