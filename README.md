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
object Users: PKTableSchema<Int>("users", primaryKey = integer("id")) {
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
object Users: DocumentSchema<Int, User>("users", javaClass(), primaryKey = integer("id")) {
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

### Polymorphic schema

Define base schema class:

```kotlin
open class ProductSchema<V, T : AbstractSchema>(javaClass: Class<V>, discriminator: String) : PolymorphicSchema<String, V>("products",
        javaClass, primaryKey = string("_id"), discriminator = Discriminator(string("type"), discriminator) ) {
    val SKU = string<T>("sku")
    val Title = string<T>("title")
    val Description = string<T>("description")
    val ASIN = string<T>("asin")

    val Shipping = ShippingColumn<T>()
    val Pricing = PricingColumn<T>()

    class ShippingColumn<T : AbstractSchema>() : Column<Shipping, T>("shipping", javaClass()) {
        val Weight = integer<T>("weight")
    }

    class DimensionsColumn<V, T : AbstractSchema>() : Column<V, T>("dimensions", javaClass()) {
        val Width = integer<T>("width")
        val Height = integer<T>("height")
        val Depth = integer<T>("depth")
    }

    class PricingColumn<T : AbstractSchema>() : Column<Pricing, T>("pricing", javaClass()) {
        val List = integer<T>("list")
        val Retail = integer<T>("retail")
        val Savings = integer<T>("savings")
        val PCTSavings = integer<T>("pct_savings")
    }
}

object Products : ProductSchema<Product, Products>(javaClass(), "") {
}

abstract class Product(val sku: String, val title: String, val description: String,
                       val asin: String, val shipping: Shipping, val pricing: Pricing) {
    val id: String? = null
}

class Shipping(val weight: Int, dimensions: Dimensions) {
}

class Dimensions(val weight: Int, val height: Int, val depth: Int) {
}

class Pricing(val list: Int, val retail: Int, val savings: Int, val pctSavings: Int) {
}
```

Define inherited schema:

 ```kotlin
object Albums : ProductSchema<Album, Albums>(javaClass(), discriminator = "Audio Album") {
    val Details = DetailsColumn<Albums>()

    class DetailsColumn<T : AbstractSchema>() : Column<Details, T>("details", javaClass()) {
        val Title = string<T>("title")
        val Artist = string<T>("artist")
        val Savings = integer<T>("savings")
        val PCTSavings = integer<T>("pct_savings")
    }
}

class Album(sku: String, title: String, description: String, asin: String, shipping: Shipping, pricing: Pricing,
    val details: Details) : Product(sku, title, description, asin, shipping, pricing) {
}

class Details(val title: String, val artist: String) {
}
```

Insert a document:

 ```kotlin
Products insert {
    Album(sku = "00e8da9b", title = "A Love Supreme", description = "by John Coltrane",
            asin = "B0000A118M", shipping = Shipping(weight = 6, dimensions = Dimensions(10, 10, 1)),
            pricing = Pricing(list = 1200, retail = 1100, savings = 100, pctSavings = 8),
            details = Details(title = "A Love Supreme [Original Recording Reissued]",
                    artist = "John Coltrane"))
}
```

Receive a collection of documents by a filter expression:

```kotlin
for (product in Products filter { SKU eq "00e8da9b" }) {
    if (product is Album) {
        println("Found music album ${product.title}")
    }
}
```

```kotlin
Albums filter { Details.Artist eq "John Coltrane" } forEach { album ->
    println("Found music album ${album.title} by John Coltrane")
}
```

Receive a document by its id:

```kotlin
val album = Albums get { id }
println("Album tracks: ${album.details.tracks}")
```