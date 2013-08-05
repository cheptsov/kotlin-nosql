Kotlin SQL Library
==================

_Exposed_ is a prototype for a lightweight SQL library written over JDBC driver for [Kotlin](https://github.com/JetBrains/kotlin) language.

```java
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

        val saintPetersburgId = Cities.insert { values("St. Petersburg")} get { id }
        val munichId = Cities.insert {values("Munich")} get { id }
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
    SQL: DROP TABLE Users
    SQL: DROP TABLE Cities