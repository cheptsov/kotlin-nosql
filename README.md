# Kotlin NoSQL

Kotlin NoSQL is a reactive and type-safe DSL for working with NoSQL databases.

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
object Comments: Schema<Comment>("comments", javaClass()) {
    val discussionId = id("discussion_id", Discussions)
    val slug = string("slug")
    val fullSlug = string("full_slug")
    val posted = dateTime("posted")
    val text = string("text")

    val AuthorInfo = AuthorInfoColumn()

    class AuthorInfoColumn() : Column<AuthorInfo, Comments>("author", javaClass()) {
        val authorId = id("id", Authors)
        val name = string("name")
    }
}

class Comment(val discussionId: Id<String, Discussions>, val slug: String,
              val fullSlug: String, posted: DateTime, text: String, authorInfo: AuthorInfo) {
    val id: Id<String, Comments>? = null
}

class AuthorInfo(val authorId: Id<String, Authors>, val name: String)
```

#### Define a database

```kotlin
val db = MongoDB(database = "test", schemas = array(Comments), initialization = CreateDrop(onCreate = {
    // ...
}))

db.withSession {
    // ...
}
```

#### Insert a document

```kotlin
Comments.insert(Comment(DiscussionId, slug, fullSlug, posted, text, AuthorInfo(author.id, author.name))).subscribe ( commentId ->
}
```

#### Get a document by id

```kotlin
Comments.find { id.equal(commentId) }.subscribe { comment ->
}
```

#### Get a list of documents by a filter expression

```kotlin
Comments.find { authorInfo.id.equal(authorId) }.sortBy { posted }.skip(10).take(5).subscribe { comment ->
}
```

#### Get selected fields by document id

```kotlin
Comments.find { id.equal(commentId) }.projection { authorInfo }.subscribe { authorInfo
}
```

#### Get selected fields by a filter expression

```kotlin
Comments.find { discussionId.equal(id) }).projection { slug + fullSlug + posted + text + authorInfo }.subscribe {
    val (slug, fullSlug, posted, text, authorInfo) = it
}
```

#### Update selected fields by document id

```kotlin
Comments.find { id.equal(commentId) }.projection { posted }.update(newDate).subscribe {
}
```

```kotlin
Comments.find { id.equal(commentId) }.projection { posted + text }.update(newDate, newText).subscribe {
}
```

### Inheritance

#### Define a base schema

```kotlin
open class ProductSchema<D, S : Schema<D>(javaClass: Class<V>, discriminator: String) : Schema<V>("products",
            discriminator = Discriminator(string("type"), discriminator)) {
    val sku = string<S>("sku")
    val title = string<S>("title")
    val description = string<S>("description")
    val asin = string<S>("asin")

    val Shipping = ShippingColumn<S>()
    val Pricing = PricingColumn<S>()

    inner class ShippingColumn<S : Schema<D>>() : Column<Shipping, S>("shipping", javaClass()) {
        val weight = integer<S>("weight")
        val dimensions = DimensionsColumn<S>()
    }

    inner class DimensionsColumn<S : Schema<D>>() : Column<Dimensions, S>("dimensions", javaClass()) {
        val width = integer<S>("width")
        val height = integer<S>("height")
        val depth = integer<S>("depth")
    }

    inner class PricingColumn<S : Schema<D>>() : Column<Pricing, S>("pricing", javaClass()) {
        val list = integer<S>("list")
        val retail = integer<S>("retail")
        val savings = integer<S>("savings")
        val ptcSavings = integer<S>("pct_savings")
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
    val details = DetailsColumn()

    class DetailsColumn() : Column<Details, Albums>("details", javaClass()) {
        val title = string("title")
        val artistId = id("artistId", Artists)
        val genre = setOfString("genre")

        val tracks = TracksColumn()
    }

    class TracksColumn() : ListColumn<Track, Albums>("tracks", javaClass()) {
        val title = string("title")
        val duration = integer("duration")
    }
}

class Album(sku: String, title: String, description: String, asin: String, shipping: Shipping,
    pricing: Pricing, val details: Details) : Product(sku, title, description, asin, shipping, pricing)

class Details(val title: String, val artistId: Id<String, Artists>, val genre: Set<String>, val tracks: List<Track>)
```

#### Insert a document

```kotlin
Products.insert(Album(sku = "00e8da9b", title = "A Love Supreme", description = "by John Coltrane",
    asin = "B0000A118M", shipping = Shipping(weight = 6, dimensions = Dimensions(10, 10, 1)),
    pricing = Pricing(list = 1200, retail = 1100, savings = 100, pctSavings = 8),
    details = Details(title = "A Love Supreme [Original Recording Reissued]",
            artistId = artistId, genre = setOf("Jazz", "General"),
            tracks = listOf(Track("A Love Supreme Part I: Acknowledgement", 100),
                    Track("A Love Supreme Part II: Resolution", 200),
                    Track("A Love Supreme, Part III: Pursuance", 300))))).subscribe { productId ->
}
```

#### Access documents via an abstract schema

```kotlin
Products.find { id.equal(productId) }.subscribe { product ->
    if (product is Album) {
    }
}
```

#### Access documents via an inherited schema

```kotlin

Albums.find { details.artistId.equal(artistId) }.subscribe { album ->
}
```