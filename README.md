# Kotlin NoSQL DSL

A type-safe Kotlin DSL to access NoSQL databases.

## Examples

### Key-value schema

Define schema:

```kotlin
object Global: KeyValueSchema("global") {
    val UserId = integer("nextUserId")
    val PostId = integer("nextPostId")
}
```

Get value at key:

```kotlin
val aUserId = Global get { UserId }
```

### Primary-key table schema

Define schema:

```kotlin
object Users: PKTableSchema<Int>("users", PK.integer("id")) {
    val Name = string("username")
    val Password = string("password")
    val Posts = listOfInteger("password")
    val Followers = setOfInteger("followers")
    val Following = setOfInteger("following")
    val Auth = nullableString("auth")

    val All = Name + Password
}
```

Insert column values by a primary key:

```kotlin
val aUserId = Global next { UserId }
Users columns { All } insert { values(aUserId, "antirez", "p1pp0") }
```

Receive column values by a primary key:

```kotlin
val (name, password) = Users columns { All } get { aUserId }
```

Receive a collection of column values by a filter condition:

```kotlin
Users columns { ID } filter { Name eq "antirez" } forEach { id ->
}
```

Receive a column value by a primary key:

```kotlin
val followers = Users columns { Followers } get { aUserId } map { userId ->
    Users columns { this.Name } get { userId }
}
```

### Document schema

Define schema:

```kotlin
object Users: DocumentSchema<Int, User>("users", javaClass(), PK.integer("id")) {
    val Name = string("username")
    val Password = string("password")
    val Posts = listOfInteger("password")
    val Followers = setOfInteger("followers")
    val Following = setOfInteger("following")
    val Auth = nullableString("auth")
}

class User(val id: Int,
           val name: String,
           val password: String,
           val posts: List<Int> = listOf(),
           val followers: Set<Int> = setOf(),
           val following: Set<Int> = setOf(),
           val auth: String? = null) {
}
```

Insert a document:

```kotlin
val aUserId = Global next { UserId }
Users add { User(aUserId, "antirez", "p1pp0") }
```

Get a document by its primary key:

```kotlin
val user = Users get { aUserId }
```

Receive a collection of documents by a filter condition:

```kotlin
for (user in Users filter { Name eq "antirez" }) {
    println(user)
}
```