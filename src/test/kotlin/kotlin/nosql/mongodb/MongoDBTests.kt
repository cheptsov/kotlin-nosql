package kotlin.nosql.mongodb

import org.junit.Test
import kotlin.nosql.*

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

object Albums : ProductSchema<Album, Albums>(javaClass(), discriminator = "Audio Album") {
    val Details = DetailsColumn<Albums>()

    class DetailsColumn<T : AbstractSchema>() : Column<Details, T>("details", javaClass()) {
        val Title = string<T>("title")
        val Artist = string<T>("artist")
        val Genre = setOfString<T>("genre")

        val Tracks = TracksColumn<T>()
    }

    class TracksColumn<T: AbstractSchema>() : ListColumn<Track, T>("tracks", javaClass()) {
        val Title = string<T>("title")
        val Duration = integer<T>("duration")
    }
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

class Album(sku: String, title: String, description: String, asin: String, shipping: Shipping, pricing: Pricing,
            val details: Details) : Product(sku, title, description, asin, shipping, pricing) {
}

class Details(val title: String, val artist: String, val genre: Set<String>, val tracks: List<Track>) {
}

class Track(val title: String, val duration: Int) {
    override fun toString(): String = "$title - $duration"
}

class MongoDBTests {
    Test
    fun test() {
        val db = MongoDB(database = "test", schemas = array<AbstractSchema>(Products, Albums)) // Compiler failure

        db {
            val id = Products insert {
                Album(sku = "00e8da9b", title = "A Love Supreme", description = "by John Coltrane",
                        asin = "B0000A118M", shipping = Shipping(weight = 6, dimensions = Dimensions(10, 10, 1)),
                        pricing = Pricing(list = 1200, retail = 1100, savings = 100, pctSavings = 8),
                        details = Details(title = "A Love Supreme [Original Recording Reissued]",
                                artist = "John Coltrane", genre = setOf("Jazz", "General"),
                                tracks = listOf(Track("A Love Supreme Part I: Acknowledgement", 100),
                                        Track("A Love Supreme Part II - Resolution", 200),
                                        Track("A Love Supreme, Part III: Pursuance", 300),
                                        Track("A Love Supreme, Part IV-Psalm", 400))))
            }

            println("Getting products by a filter expression:")

            for (product in Products filter { (SKU eq "00e8da9b") or (Shipping.Weight eq 6) }) {
                if (product is Album) {
                    println("Found music album ${product.details.title}")
                }
            }

            println("Getting albums by John Coltrane:")

            Albums filter { Details.Artist eq "John Coltrane" } forEach { album ->
                println("Found music album ${album.details.title}")
            }

            println("Getting an album by its id:")

            val album = Albums get { id }
            println("Album tracks: ${album.details.tracks}")

            println("Getting selected columns of a document bt its id:")

            val (title, pricing) = Albums columns { Details.Title + Pricing } get { id }
            println("Retail price for the album ${title} is ${pricing.retail}")

            Products delete { ID eq id }
        }
    }
}