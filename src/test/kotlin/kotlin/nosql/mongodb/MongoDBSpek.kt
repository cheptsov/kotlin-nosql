package kotlin.nosql.mongodb

import org.spek.Spek
import kotlin.test.assertEquals
import kotlin.nosql.*

class MongoDBSpek : Spek() {
    open class ProductSchema<V, T : Schema>(javaClass: Class<V>, discriminator: String) : PolymorphicSchema<String, V>("products",
            javaClass, primaryKey = string("_id"), discriminator = Discriminator(string("type"), discriminator)) {
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
            val Height = nullableInteger<T>("height")
            val Depth = integer<T>("depth")
        }

        class PricingColumn<T : Schema>() : Column<Pricing, T>("pricing", javaClass()) {
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

        class DetailsColumn<T : Schema>() : Column<Details, T>("details", javaClass()) {
            val Title = string<T>("title")
            val Artist = string<T>("artist")
            val Genre = setOfString<T>("genre")

            val Tracks = TracksColumn<T>()
        }

        class TracksColumn<T : Schema>() : ListColumn<Track, T>("tracks", javaClass()) {
            val Title = string<T>("title")
            val Duration = integer<T>("duration")
        }
    }

    abstract class Product(val sku: String, val title: String, val description: String,
                           val asin: String, val shipping: Shipping, val pricing: Pricing) {
        val id: String? = null
    }

    class Shipping(val weight: Int, val dimensions: Dimensions) {
    }

    class Dimensions(val width: Int, val height: Int?, val depth: Int) {
    }

    class Pricing(val list: Int, val retail: Int, val savings: Int, val pctSavings: Int) {
    }

    class Album(sku: String, title: String, description: String, asin: String, shipping: Shipping, pricing: Pricing,
                val details: Details) : Product(sku, title, description, asin, shipping, pricing) {
    }

    class Details(val title: String, val artist: String, val genre: Set<String>, val tracks: List<Track>) {
    }

    class Track(val title: String, val duration: Int) {
    }

    {
        val original_album = Album(sku = "00e8da9b", title = "A Love Supreme", description = "by John Coltrane",
                asin = "B0000A118M", shipping = Shipping(weight = 6, dimensions = Dimensions(10, 10, 1)),
                pricing = Pricing(list = 1200, retail = 1100, savings = 100, pctSavings = 8),
                details = Details(title = "A Love Supreme [Original Recording Reissued]",
                        artist = "John Coltrane", genre = setOf("Jazz", "General"),
                        tracks = listOf(Track("A Love Supreme Part I: Acknowledgement", 100),
                                Track("A Love Supreme Part II - Resolution", 200),
                                Track("A Love Supreme, Part III: Pursuance", 300))))

        given("a polymorhpic schema") {
            val db = MongoDB(database = "test", schemas = array<Schema>(Products, Albums)) // Compiler failure
            db {
                Products.drop()
            }
            var albumId: String? = null

            on("inserting a document") {
                db {
                    val id = Products insert { original_album }
                    it("should return a generated id") {
                        assert(id.length > 0)
                    }
                    albumId = id
                }
            }

            fun validate(results: List<Product>) {
                assert(results.size == 1)
                assert(results[0] is Album)
                val album = results[0] as Album
                assertEquals("00e8da9b", results[0].sku)
                assertEquals("A Love Supreme", results[0].title)
                assertEquals("by John Coltrane", results[0].description)
                assertEquals("B0000A118M", results[0].asin)
                assertEquals(6, results[0].shipping.weight)
                assertEquals(10, results[0].shipping.dimensions.width)
                assertEquals(10, results[0].shipping.dimensions.height)
                assertEquals(1, results[0].shipping.dimensions.depth)
                assertEquals(1200, results[0].pricing.list)
                assertEquals(1100, results[0].pricing.retail)
                assertEquals(100, results[0].pricing.savings)
                assertEquals(8, results[0].pricing.pctSavings)
                assertEquals("A Love Supreme [Original Recording Reissued]", album.details.title)
                assertEquals("John Coltrane", album.details.artist)
                assert(album.details.genre.size == 2)
                assert(album.details.genre.contains("Jazz"))
                assert(album.details.genre.contains("General"))
                assert(album.details.tracks.size == 3)
                assertEquals(album.details.tracks[0].title, "A Love Supreme Part I: Acknowledgement")
                assertEquals(album.details.tracks[0].duration, 100)
                assertEquals(album.details.tracks[1].title, "A Love Supreme Part II - Resolution")
                assertEquals(album.details.tracks[1].duration, 200)
                assertEquals(album.details.tracks[2].title, "A Love Supreme, Part III: Pursuance")
                assertEquals(album.details.tracks[2].duration, 300)
            }

            on("filtering an abstract schema") {
                db {
                    val results = (Products filter { (SKU eq "00e8da9b") or (Shipping.Weight eq 6) }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering a non-abstract schema") {
                db {
                    val results: List<Album> = (Albums filter { Details.Artist eq "John Coltrane" }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("getting a document by id") {
                db {
                    val album = Albums get { albumId!! }
                    it("should return a correct object") {
                        validate(listOf(album))
                    }
                }
            }

            on("getting one column by id") {
                db {
                    val title = Albums columns { Details.Title } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                    }
                }
            }

            on("getting two columns by id") {
                db {
                    val (title, pricing) = Albums columns { Details.Title + Pricing } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                        assertEquals(1200, pricing.list)
                        assertEquals(1100, pricing.retail)
                        assertEquals(100, pricing.savings)
                        assertEquals(8, pricing.pctSavings)
                    }
                }
            }

            on("getting three columns by id") {
                db {
                    val (sku, title, pricing) = Albums columns { SKU + Details.Title + Pricing } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                        assertEquals(1200, pricing.list)
                        assertEquals(1100, pricing.retail)
                        assertEquals(100, pricing.savings)
                        assertEquals(8, pricing.pctSavings)
                    }
                }
            }

            on("getting four columns by id") {
                db {
                    val (sku, title, description, pricing) = Products columns { SKU + Title + Description + Pricing } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals(1200, pricing.list)
                        assertEquals(1100, pricing.retail)
                        assertEquals(100, pricing.savings)
                        assertEquals(8, pricing.pctSavings)
                    }
                }
            }

            on("getting five columns by id") {
                db {
                    val (sku, title, description, asin, pricing) = Products columns { SKU + Title + Description + ASIN + Pricing } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, pricing.list)
                        assertEquals(1100, pricing.retail)
                        assertEquals(100, pricing.savings)
                        assertEquals(8, pricing.pctSavings)
                    }
                }
            }

            on("getting six columns by id") {
                db {
                    val (sku, title, description, asin, list, retail) = Products columns { SKU + Title +
                        Description + ASIN + Pricing.List + Pricing.Retail } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                    }
                }
            }

            on("getting seven columns by id") {
                db {
                    val (sku, title, description, asin, list, retail, savings) = Products columns { SKU + Title +
                        Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                        assertEquals(100, savings)
                    }
                }
            }

            on("getting eight columns by id") {
                db {
                    val (sku, title, description, asin, list, retail, savings, pctSavings) = Products columns {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings
                    } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                        assertEquals(100, savings)
                        assertEquals(8, pctSavings)
                    }
                }
            }

            on("getting nine columns by id") {
                db {
                    val (sku, title, description, asin, list, retail, savings, pctSavings, shipping) = Products columns {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings + Shipping
                    } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                        assertEquals(100, savings)
                        assertEquals(8, pctSavings)
                        assertEquals(6, shipping.weight)
                        assertEquals(10, shipping.dimensions.width)
                        assertEquals(10, shipping.dimensions.height)
                        assertEquals(1, shipping.dimensions.depth)
                    }
                }
            }

            on("getting ten columns by id") {
                db {
                    val (sku, title, description, asin, list, retail, savings, pctSavings, weight, dimensions) = Products columns {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings + Shipping.Weight + Shipping.Dimensions
                    } get { albumId!! }
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                        assertEquals(100, savings)
                        assertEquals(8, pctSavings)
                        assertEquals(6, weight)
                        assertEquals(10, dimensions.width)
                        assertEquals(10, dimensions.height)
                        assertEquals(1, dimensions.depth)
                    }
                }
            }

            on("getting one column by filter expression") {
                db {
                    val title = (Albums columns { Details.Title } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                    }
                }
            }

            on("getting two columns by a filter expression") {
                db {
                    val (title, pricing) = (Albums columns { Details.Title + Pricing } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                        assertEquals(1200, pricing.list)
                        assertEquals(1100, pricing.retail)
                        assertEquals(100, pricing.savings)
                        assertEquals(8, pricing.pctSavings)
                    }
                }
            }

            on("getting three columns by a filter expression") {
                db {
                    val (sku, title, pricing) = (Albums columns { SKU + Details.Title + Pricing } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                        assertEquals(1200, pricing.list)
                        assertEquals(1100, pricing.retail)
                        assertEquals(100, pricing.savings)
                        assertEquals(8, pricing.pctSavings)
                    }
                }
            }

            on("getting four columns by a filter expression") {
                db {
                    val (sku, title, description, pricing) = (Products columns { SKU + Title + Description + Pricing } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals(1200, pricing.list)
                        assertEquals(1100, pricing.retail)
                        assertEquals(100, pricing.savings)
                        assertEquals(8, pricing.pctSavings)
                    }
                }
            }

            on("getting five columns by a filter expression") {
                db {
                    val (sku, title, description, asin, pricing) = (Products columns { SKU + Title + Description + ASIN + Pricing } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, pricing.list)
                        assertEquals(1100, pricing.retail)
                        assertEquals(100, pricing.savings)
                        assertEquals(8, pricing.pctSavings)
                    }
                }
            }

            on("getting six columns by a filter expression") {
                db {
                    val (sku, title, description, asin, list, retail) = (Products columns { SKU + Title +
                        Description + ASIN + Pricing.List + Pricing.Retail } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                    }
                }
            }

            on("getting seven columns by a filter expression") {
                db {
                    val (sku, title, description, asin, list, retail, savings) = (Products columns { SKU + Title +
                        Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                        assertEquals(100, savings)
                    }
                }
            }

            on("getting eight columns by a filter expression") {
                db {
                    val (sku, title, description, asin, list, retail, savings, pctSavings) = (Products columns {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings
                    } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                        assertEquals(100, savings)
                        assertEquals(8, pctSavings)
                    }
                }
            }

            on("getting nine columns by a filter expression") {
                db {
                    val (sku, title, description, asin, list, retail, savings, pctSavings, shipping) = (Products columns {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings + Shipping
                    } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                        assertEquals(100, savings)
                        assertEquals(8, pctSavings)
                        assertEquals(6, shipping.weight)
                        assertEquals(10, shipping.dimensions.width)
                        assertEquals(10, shipping.dimensions.height)
                        assertEquals(1, shipping.dimensions.depth)
                    }
                }
            }

            on("getting ten columns by a filter expression") {
                db {
                    val (sku, title, description, asin, list, retail, savings, pctSavings, weight, dimensions) = (Products columns {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings + Shipping.Weight + Shipping.Dimensions
                    } filter { SKU eq "00e8da9b" }).first()
                    it("returns correct values") {
                        assertEquals("00e8da9b", sku)
                        assertEquals("A Love Supreme", title)
                        assertEquals("by John Coltrane", description)
                        assertEquals("B0000A118M", asin)
                        assertEquals(1200, list)
                        assertEquals(1100, retail)
                        assertEquals(100, savings)
                        assertEquals(8, pctSavings)
                        assertEquals(6, weight)
                        assertEquals(10, dimensions.width)
                        assertEquals(10, dimensions.height)
                        assertEquals(1, dimensions.depth)
                    }
                }
            }

            on("filtering an abstract schema by equal expression") {
                db {
                    val results = (Products filter { Shipping.Weight eq 6 }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression") {
                db {
                    val results = (Products filter { Shipping.Weight eq 7 }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by notEqual expression") {
                db {
                    val results = (Products filter { Shipping.Weight ne 7 }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by notEqual expression") {
                db {
                    val results = (Products filter { Shipping.Weight ne 6 }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by gt expression") {
                db {
                    val results = (Products filter { Shipping.Weight gt 5 }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by gt expression") {
                db {
                    val results = (Products filter { Shipping.Weight gt 6 }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by lt expression") {
                db {
                    val results = (Products filter { Shipping.Weight lt 7 }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by lt expression") {
                db {
                    val results = (Products filter { Shipping.Weight lt 6 }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db {
                    val results = (Products filter { Shipping.Weight ge 6 }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db {
                    val results = (Products filter { Shipping.Weight ge 5 }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db {
                    val results = (Products filter { Shipping.Weight ge 7 }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db {
                    val results = (Products filter { Shipping.Weight le 6 }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db {
                    val results = (Products filter { Shipping.Weight le 7 }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db {
                    val results = (Products filter { Shipping.Weight le 5 }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by mb expression") {
                db {
                    val results = (Products filter { Shipping.Weight mb array(5, 6) }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by mb expression") {
                db {
                    val results = (Products filter { Shipping.Weight mb array(5, 7) }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by nm expression") {
                db {
                    val results = (Products filter { Shipping.Weight nm array(5, 7) }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by nm expression") {
                db {
                    val results = (Products filter { Shipping.Weight nm array(5, 6) }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db {
                    val results = (Products filter { with (Shipping.Dimensions) { Width eq Height } }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db {
                    val results = (Products filter { with (Shipping.Dimensions) { Width eq Depth } }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db {
                    val results = (Products filter { with (Shipping.Dimensions) { Width ne Depth } }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db {
                    val results = (Products filter { with (Shipping.Dimensions) { Width ne Height } }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            //

            on("filtering an abstract schema by gt expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Width gt Shipping.Dimensions.Depth }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by gt expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Depth gt Shipping.Dimensions.Width }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by lt expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Depth lt Shipping.Dimensions.Width }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by lt expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Width lt Shipping.Dimensions.Depth }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Width ge Shipping.Dimensions.Height }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Width ge Shipping.Dimensions.Depth }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Depth ge Shipping.Dimensions.Width }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Depth le Shipping.Dimensions.Width }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Width le Shipping.Dimensions.Height }).toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db {
                    val results = (Products filter { Shipping.Dimensions.Width le Shipping.Dimensions.Depth }).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("setting a new value to a string column on a non-abstract schema by id") {
                db {
                    Albums columns { Details.Title } at { albumId!! } set { "A Love Supreme. Original Recording Reissued" }
                    val title = Albums columns { Details.Title } get { albumId!! }
                    it("takes effect") {
                        assertEquals("A Love Supreme. Original Recording Reissued", title)
                    }
                }
            }

            on("setting a new value to a string column on a non-abstract schema by id") {
                db {
                    Albums columns { Details.Title } at { albumId!! } set { "A Love Supreme. Original Recording Reissued" }
                    val title = Albums columns { Details.Title } get { albumId!! }
                    it("takes effect") {
                        assertEquals("A Love Supreme. Original Recording Reissued", title)
                    }
                }
            }

            on("setting values to a few integer columns on an abstract schema by a filter expression") {
                db {
                    Products columns { Pricing.Retail + Pricing.Savings } filter { SKU eq "00e8da9b" } set { values(1150, 50) }
                    val (retail, savings)= Products columns { Pricing.Retail + Pricing.Savings } get { albumId!! }
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                    }
                }
            }

            on("adding a new element to a list column on a non-abstract schema by id") {
                db {
                    Albums columns { Details.Tracks } at { albumId!! } add { Track("A Love Supreme, Part IV-Psalm", 400) }
                    val tracks = Albums columns { Albums.Details.Tracks } get { albumId!! }
                    it("takes effect") {
                        assertEquals(4, tracks.size)
                        assertEquals("A Love Supreme, Part IV-Psalm", tracks[3].title)
                        assertEquals(400, tracks[3].duration)
                    }
                }
            }

            on("deleting a document") {
                db {
                    Products delete { ID eq albumId!! }
                    it("deletes the document from database") {
                        assert((Albums filter { pk eq albumId!! }).toList().isEmpty())
                    }
                }
            }
        }
    }
}

