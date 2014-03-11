# Kotlin NoSQL

Kotlin NoSQL is a NoSQL database query and access library for [Kotlin](http://github.com/JetBrains/Kotlin) language.
It offers a powerful DSL for working with key-value, column and document NoSQL databases.

The key principles of Kotlin NoSQL:

### First-class query

### Immutability

### Type-safety

The following NoSQL databases are supported for now:

- MongoDB

## Download

To use it with Maven insert the following in your pom.xml file:

```xml
<dependency>
    <groupId>org.jetbrains.kotlin</groupId>
    <artifactId>kotlin-nosql</artifactId>
    <version>$version</version>
 </dependency>
```

To use it with Gradle insert the following in your build.gradle:

```groovy
dependencies {
    compile 'org.jetbrains.kotlin:kotlin-nosql:$version'
}
```

## Getting Started

### Basics

#### Define a schema

```kotlin
object Comments: MongoDBSchema<Comment>("comments", javaClass()) {
    val DiscussionID = objectId("discussion_id")
    val Slug = string("slug")
    val FullSlug = string("full_slug")
    val Posted = dateTime("posted")
    val Text = string("text")

    val AuthorInfo = AuthorInfoColumn()

    class AuthorInfoColumn() : Column<AuthorInfo, Comments>("author", javaClass()) {
        val ID = objectId("id")
        val Name = string("name")
    }
}

class Comment(val: id: ObjectId, val discussionId: ObjectId, val slug: String,
    val fullSlug: String, posted: DateTime, text: String, authorInfo: AuthorInfo)

class AuthorInfo(val: id: ObjectId, val name: String)
```

#### Define a database

```kotlin
val db = MongoDB(database = "test", schemas = array(Comments))
```

#### Insert a document

```kotlin
db {
    val commentId = Comments insert Comment(discussionId, slug, fullSlug, posted,
        text, AuthorInfo(author.id, author.name))
}
```

#### Get a document by id

```kotlin
db {
    val comment = Comments get commentId
}
```

#### Get a list of documents by a filter expression

```kotlin
db {
    for (comment in Comments filter { AuthorInfo.ID eq author.id } sort { Posted } drop 10 take 5) {
    }
}
```

#### Get selected fields by document id

```kotlin
db {
    val authorInfo = Comments columns { AuthorInfo } get commentId
}
```

#### Get selected fields by a filter expression

```kotlin
db {
    for ((slug, fullSlug, posted, text, authorInfo) in Comments columns { Slug +
        FullSlug + Posted + Text + AuthorInfo } filter { DiscussionID eq discussion Id }) {
    }
}
```

#### Update selected fields by document id

```kotlin
db {
    Comments columns { Posted } find commentId set newDate
}
```

```kotlin
db {
    Comments columns { Posted + Text } find commentId set values(newDate, newText)
}
```
