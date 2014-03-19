# Kotlin NoSQL

Kotlin NoSQL is a NoSQL database query and access library for [Kotlin](http://github.com/JetBrains/Kotlin) language.
It offers a powerful and type-safe DSL for working with key-value, column and document NoSQL databases.

## Principles

The following key principles lie behind Kotlin NoSQL:

#### First-class query

Unlike to ORM frameworks with its object persistence strategy Kotlin NoSQL uses another approach: immutability and
queries. Each operation on data may be described via a statically-typed query:

```kotlin
Albums.select { Details.Tracks }.filter { Details.ArtistId.eq(artistId) }.delete { Duration.lt(200) }
```

#### Type-safety

Once you have a schema defined you can access documents with queries, always getting type-safe results:

```kotlin
for (product in Products.filter { Pricing.Savings.ge(1000) }) {
    when (product) {
        is Album -> // ...
        else -> // ...
    }
}
```

```kotlin
for ((slug, fullSlug, posted, text, authorInfo) in Comments.select { Slug +
    FullSlug + Posted + Text + AuthorInfo }.filter { DiscussionId.eq(discussionId) }) {
}
```

#### Immutability

Queries enable you to access and modify any part of document(s), without loading and changing its state in memory:

```kotlin
Products.select { Pricing.Retail + Pricing.Savings }.find(productId).set(newRetail, newSavings)
```

## Status

Under development (POC). The following NoSQL databases are supported now:

- [MongoDB](https://www.mongodb.org/)

Feedback is welcome.

## Download

To use it with Maven insert the following code in your pom.xml file:

```xml
<dependency>
    <groupId>org.jetbrains.kotlin</groupId>
    <artifactId>kotlin-nosql-mongodb</artifactId>
    <version>0.1-SNAPSHOT</version>
 </dependency>

 <repositories>
     <repository>
       <id>kotlin-nosql</id>
       <url>http://repository.jetbrains.com/kotlin-nosql</url>
     </repository>
</repositories>
```

To use it with Gradle insert the following code in your build.gradle:

```groovy
repositories {
    maven {
        url "http://repository.jetbrains.com/kotlin-nosql"
    }
}

dependencies {
    compile 'org.jetbrains.kotlin:kotlin-nosql-mongodb:0.1-SNAPSHOT'
}
```

## Getting Started

### Basics

#### Define a schema

```kotlin
object Comments: MongoDBSchema<Comment>("comments", javaClass()) {
    val DiscussionId = id("discussion_id", Discussions)
    val Slug = string("slug")
    val FullSlug = string("full_slug")
    val Posted = dateTime("posted")
    val Text = string("text")

    val AuthorInfo = AuthorInfoColumn()

    class AuthorInfoColumn() : Column<AuthorInfo, Comments>("author", javaClass()) {
        val Id = id("id", Authors)
        val Name = string("name")
    }
}

class Comment(val id: Id<String, Comments>? = null,
              val DiscussionId: Id<String, Discussions>, val slug: String,
              val fullSlug: String, posted: DateTime, text: String, authorInfo: AuthorInfo) {

}

class AuthorInfo(val id: Id<String, Authors>, val name: String)
```

#### Define a database

```kotlin
val db = MongoDB(database = "test", schemas = array(Comments))

db {
    // ...
}
```

#### Insert a document

```kotlin
val commentId = Comments.insert(Comment(DiscussionId, slug, fullSlug, posted,
    text, AuthorInfo(author.id, author.name)))
```

#### Get a document by id

```kotlin
val comment = Comments.get(commentId)
```

#### Get a list of documents by a filter expression

```kotlin
for (comment in Comments.filter { AuthorInfo.Id eq authorId }.sortBy { Posted }. drop(10).take(5)) {
}
```

#### Get selected fields by document id

```kotlin
val authorInfo = Comments.select { AuthorInfo }.get(commentId)
```

#### Get selected fields by a filter expression

```kotlin
for ((slug, fullSlug, posted, text, authorInfo) in Comments.select { Slug +
    FullSlug + Posted + Text + AuthorInfo }.filter { DiscussionId.equal(discussionId) }) {
}
```

#### Update selected fields by document id

```kotlin
Comments.select { Posted }.find(commentId).set(newDate)
```

```kotlin
Comments.select { Posted + Text }.find(commentId).set(newDate, newText)
```

### Inheritance

#### Define a base schema

```kotlin
open class ProductSchema<V, T : Schema>(javaClass: Class<V>, discriminator: String) : MongoDBSchema<V>("products",
            discriminator = Discriminator(string("type"), discriminator)) {
    val SKU = string<T>("sku")
    val Title = string<T>("title")
    val Description = string<T>("description")
    val ASIN = string<T>("asin")

    val Shipping = ShippingColumn<T>()
    val Pricing = PricingColumn<T>()

    class ShippingColumn<T : Schema>() : Column<Shipping, T>("shipping", javaClass()) {
        val Weight = integer<T>("weight")
        val Dimensions = DimensionsColumn<T>()
    }

    class DimensionsColumn<T : Schema>() : Column<Dimensions, T>("dimensions", javaClass()) {
        val Width = integer<T>("width")
        val Height = integer<T>("height")
        val Depth = integer<T>("depth")
    }

    class PricingColumn<T : Schema>() : Column<Pricing, T>("pricing", javaClass()) {
        val List = integer<T>("list")
        val Retail = integer<T>("retail")
        val Savings = integer<T>("savings")
        val PCTSavings = integer<T>("pct_savings")
    }
}

object Products : ProductSchema<Product, Products>(javaClass(), "")

abstract class Product(val id: Id<String, Products>? = null, val sku: String, val title: String, val description: String,
                       val asin: String, val shipping: Shipping, val pricing: Pricing) {
    val id: Id<String, Products>? = null
}

class Shipping(val weight: Int, val dimensions: Dimensions)

class Dimensions(val width: Int, val height: Int, val depth: Int)

class Pricing(val list: Int, val retail: Int, val savings: Int, val pctSavings: Int)
```

#### Define an inherited schema

```kotlin
object Albums : ProductSchema<Album, Albums>(javaClass(), discriminator = "Audio Album") {
    val Details = DetailsColumn()

    class DetailsColumn() : Column<Details, Albums>("details", javaClass()) {
        val Title = string("title")
        val ArtistId = id("artistId", Artists)
        val Genre = setOfString("genre")

        val Tracks = TracksColumn()
    }

    class TracksColumn() : ListColumn<Track, Albums>("tracks", javaClass()) {
        val Title = string("title")
        val Duration = integer("duration")
    }
}

class Album(sku: String, title: String, description: String, asin: String, shipping: Shipping,
    pricing: Pricing, val details: Details) : Product(sku, title, description, asin, shipping, pricing)

class Details(val title: String, val artistId: Id<String, Artists>, val genre: Set<String>, val tracks: List<Track>)
```

#### Insert a document

```kotlin
val productId = Products.insert(Album(sku = "00e8da9b", title = "A Love Supreme", description = "by John Coltrane",
    asin = "B0000A118M", shipping = Shipping(weight = 6, dimensions = Dimensions(10, 10, 1)),
    pricing = Pricing(list = 1200, retail = 1100, savings = 100, pctSavings = 8),
    details = Details(title = "A Love Supreme [Original Recording Reissued]",
            artistId = artistId, genre = setOf("Jazz", "General"),
            tracks = listOf(Track("A Love Supreme Part I: Acknowledgement", 100),
                    Track("A Love Supreme Part II: Resolution", 200),
                    Track("A Love Supreme, Part III: Pursuance", 300)))))
```

#### Access documents via an abstract schema

```kotlin
val product = Products.get(productId)
    if (product is Album) {
        // ...
    }
}
```

#### Access documents via an inherited schema

```kotlin
for (albums in Albums.filter { Details.ArtistId.equal(artistId) }) {
    // ...
}
```