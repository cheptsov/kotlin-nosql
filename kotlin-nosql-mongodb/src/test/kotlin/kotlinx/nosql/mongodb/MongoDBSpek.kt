package kotlinx.nosql.mongodb

import org.spek.Spek
import kotlin.test.assertEquals
import kotlinx.nosql.*
import org.joda.time.LocalDate

class MongoDBSpek : Spek() {
    open class ProductSchema<V, T : Schema>(javaClass: Class<V>, discriminator: String) : MongoDBSchema<V>("products",
            javaClass, discriminator = Discriminator(string("type"), discriminator)) {
        val SKU = string<T>("sku")
        val Title = string<T>("title")
        val Description = string<T>("description")
        val ASIN = string<T>("asin")
        val Available = boolean<T>("available")
        val CreatedAtDate = date<T>("createdAtDate")
        val NullableBooleanNoValue = nullableBoolean<T>("nullableBooleanNoValue")
        val NullableBooleanWithValue = nullableBoolean<T>("nullableBooleanWithValue")
        val NullableDateNoValue = nullableDate<T>("nullableDateNoValue")
        val NullableDateWithValue = nullableDate<T>("nullableDateWithValue")
        val Cost = double<T>("cost")
        val NullableDoubleNoValue = nullableDouble<T>("nullableDoubleNoValue")
        val NullableDoubleWithValue = nullableDouble<T>("nullableDoubleWithValue")

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

    object Artists : MongoDBSchema<Artist>("artists", javaClass()) {
        val Name = string("name")
    }

    object Products : ProductSchema<Product, Products>(javaClass(), "") {
    }

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

    abstract class Product(val sku: String, val title: String, val description: String,
                           val asin: String, val available: Boolean, val cost: Double,
                           val createdAtDate: LocalDate, val nullableBooleanNoValue: Boolean?,
                           val nullableBooleanWithValue: Boolean?,
                           val nullableDateNoValue: LocalDate?, val nullableDateWithValue: LocalDate?,
                           val nullableDoubleNoValue: Double?, val nullableDoubleWithValue: Double?,
                           val shipping: Shipping, val pricing: Pricing) {
        val id: Id<String, Products>? = null
    }

    class Shipping(val weight: Int, val dimensions: Dimensions) {
    }

    class Dimensions(val width: Int, val height: Int, val depth: Int) {
    }

    class Pricing(val list: Int, val retail: Int, val savings: Int, val pctSavings: Int) {
    }

    class Album(sku: String, title: String, description: String, asin: String, available: Boolean,
                cost: Double, createdAtDate: LocalDate,
                nullableBooleanNoValue: Boolean?, nullableBooleanWithValue: Boolean?,
                nullableDateNoValue: LocalDate?, nullableDateWithValue: LocalDate?,
                nullableDoubleNoValue: Double?, nullableDoubleWithValue: Double?, shipping: Shipping, pricing: Pricing,
                val details: Details) : Product(sku, title, description, asin, available, cost, createdAtDate,
            nullableBooleanNoValue, nullableBooleanWithValue, nullableDateNoValue, nullableDateWithValue,
            nullableDoubleNoValue, nullableDoubleWithValue, shipping, pricing) {
    }

    class Artist(val name: String) {
        val id: Id<String, Artists>? = null
    }

    class Details(val title: String, val artistId: Id<String, Artists>, val genre: Set<String>, val tracks: List<Track>) {
    }

    class Track(val title: String, val duration: Int) {
    }

    {
        given("a polymorhpic schema") {
            val db = MongoDB(schemas = array<Schema>(Artists, Products, Albums)) // Compiler failure

            db {
                array(Products, Artists).forEach { it.drop() }
            }
            var artistId: Id<String, Artists>? = null
            var albumId: Id<String, Albums>? = null

            on("inserting a document") {
                db {
                    val arId: Id<String, Artists> = Artists insert Artist(name = "John Coltrane")
                    it("should return a generated id for artist") {
                        assert(arId.value.length > 0)
                    }
                    val aId = Albums.insert(Album(sku = "00e8da9b", title = "A Love Supreme", description = "by John Coltrane",
                            asin = "B0000A118M", available = true, cost = 1.23, createdAtDate = LocalDate(2014, 3, 8), nullableBooleanNoValue = null,
                            nullableBooleanWithValue = false, nullableDateNoValue = null, nullableDateWithValue = LocalDate(2014, 3, 7),
                            nullableDoubleNoValue = null, nullableDoubleWithValue = 1.24,
                            shipping = Shipping(weight = 6, dimensions = Dimensions(10, 10, 1)),
                            pricing = Pricing(list = 1200, retail = 1100, savings = 100, pctSavings = 8),
                            details = Details(title = "A Love Supreme [Original Recording Reissued]",
                                    artistId = arId, genre = setOf("Jazz", "General"),
                                    tracks = listOf(Track("A Love Supreme Part I: Acknowledgement", 100),
                                            Track("A Love Supreme Part II - Resolution", 200),
                                            Track("A Love Supreme, Part III: Pursuance", 300)))))
                    it("should return a generated id for album") {
                        assert(aId.value.length > 0)
                    }
                    albumId = aId
                    artistId = arId
                }
            }

            on("filtering a non-inherited schema") {
                db {
                    val artists = Artists.filter { Name.equal("John Coltrane") }.toList()
                    it("should return a generated id for artist") {
                        assert(artists.size == 1)
                    }
                }
            }

            fun validate(results: List<Product>) {
                assert(results.size == 1)
                assert(results[0] is Album)
                val album = results[0] as Album
                assertEquals("00e8da9b", results[0].sku)
                assertEquals(true, results[0].available)
                assertEquals(1.23, results[0].cost)
                assertEquals(LocalDate(2014, 3, 8), results[0].createdAtDate)
                assert(results[0].nullableDateNoValue == null)
                assertEquals(LocalDate(2014, 3, 7), results[0].nullableDateWithValue)
                assert(results[0].nullableDoubleNoValue == null)
                assertEquals(1.24, results[0].nullableDoubleWithValue)
                assert(results[0].nullableBooleanNoValue == null)
                assertEquals(false, results[0].nullableBooleanWithValue)
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
                assertEquals(artistId!!, album.details.artistId)
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
                    val results = Products.filter { (SKU.equal("00e8da9b")).or(Shipping.Weight.equal(6)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering a non-abstract schema") {
                db {
                    val results: List<Album> = Albums.filter { Details.ArtistId.equal(artistId!!) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering a non-abstract schema drop take") {
                db {
                    val results = Products.filter { (SKU.equal("00e8da9b")).or(Shipping.Weight.equal(6)) }.drop(1).take(1).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("getting a document by id") {
                db {
                    val album = Albums.get(albumId!!)
                    it("should return a correct object") {
                        validate(listOf(album))
                    }
                }
            }

            on("getting one column by id") {
                db {
                    val title = Albums.select { Details.Title }.get(albumId!!)
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                    }
                }
            }

            on("getting two columns by id") {
                db {
                    val (title, pricing) = Albums.select { Details.Title + Pricing }.get(albumId!!)
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
                    val (sku, title, pricing) = Albums.select { SKU + Details.Title + Pricing }.get(albumId!!)
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
                    val (sku, title, description, pricing) = Albums.select { SKU + Title + Description + Pricing }.get(albumId!!)
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
                    val (sku, title, description, asin, pricing) = Albums.select { SKU + Title + Description + ASIN + Pricing }.get(albumId!!)
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
                    val (sku, title, description, asin, list, retail) = Albums.select { SKU + Title +
                        Description + ASIN + Pricing.List + Pricing.Retail }.get(albumId!!)
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
                    val (sku, title, description, asin, list, retail, savings) = Albums.select { SKU + Title +
                        Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings }.get(albumId!!)
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings) = Albums.select {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings
                    }.get(albumId!!)
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings, shipping) = Albums.select {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings + Shipping
                    }.get(albumId!!)
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings, weight, dimensions) = Albums.select {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings + Shipping.Weight + Shipping.Dimensions
                    }.get(albumId!!)
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
                    val title = Albums.select { Details.Title }.filter { SKU.equal("00e8da9b") }.first()
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                    }
                }
            }

            on("getting two columns by a filter expression") {
                db {
                    val (title, pricing) = Albums.select { Details.Title + Pricing }.filter { SKU.equal("00e8da9b") }.first()
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
                    val (sku, title, pricing) = Albums.select { SKU + Details.Title + Pricing }.filter { SKU.equal("00e8da9b") }.first()
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
                    val (sku, title, description, pricing) = Products.select { SKU + Title + Description + Pricing }.filter { SKU.equal("00e8da9b") }.first()
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
                    val (sku, title, description, asin, pricing) = Products.select { SKU + Title + Description + ASIN + Pricing }.filter { SKU.equal("00e8da9b") }.first()
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
                    val (sku, title, description, asin, list, retail) = Products.select { SKU + Title +
                        Description + ASIN + Pricing.List + Pricing.Retail }.filter { SKU.equal("00e8da9b") }.first()
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
                    val (sku, title, description, asin, list, retail, savings) = Products.select { SKU + Title +
                        Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings }.filter { SKU.equal("00e8da9b") }.first()
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings) = Products.select {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings
                    }.filter { SKU.equal("00e8da9b") }.first()
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings, shipping) = Products.select {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings + Shipping
                    }.filter { SKU.equal("00e8da9b") }.first()
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings, weight, dimensions) = Products.select {
                        SKU + Title + Description + ASIN + Pricing.List + Pricing.Retail + Pricing.Savings +
                        Pricing.PCTSavings + Shipping.Weight + Shipping.Dimensions
                    }.filter { SKU.equal("00e8da9b") }.first()
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
                    val results = Products.filter { Shipping.Weight.equal(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression") {
                db {
                    val results = Products.filter { Shipping.Weight.equal(7) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by notEqual expression") {
                db {
                    val results = Products.filter { Shipping.Weight.notEqual(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by notEqual expression") {
                db {
                    val results = Products.filter { Shipping.Weight.notEqual(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by gt expression") {
                db {
                    val results = Products.filter { Shipping.Weight.gt(5) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by gt expression") {
                db {
                    val results = Products.filter { Shipping.Weight.gt(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by lt expression") {
                db {
                    val results = Products.filter { Shipping.Weight.lt(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by lt expression") {
                db {
                    val results = Products.filter { Shipping.Weight.lt(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db {
                    val results = Products.filter { Shipping.Weight.ge(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db {
                    val results = Products.filter { Shipping.Weight.ge(5) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db {
                    val results = Products.filter { Shipping.Weight.ge(7) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db {
                    val results = Products.filter { Shipping.Weight.le(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db {
                    val results = Products.filter { Shipping.Weight.le(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db {
                    val results = Products.filter { Shipping.Weight.le(5) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by mb expression") {
                db {
                    val results = Products.filter { Shipping.Weight.memberOf(array(5, 6)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by mb expression") {
                db {
                    val results = Products.filter { Shipping.Weight.memberOf(array(5, 7)) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by nm expression") {
                db {
                    val results = Products.filter { Shipping.Weight.notMemberOf(array(5, 7)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by nm expression") {
                db {
                    val results = Products.filter { Shipping.Weight.notMemberOf(array(5, 6)) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db {
                    val results = Products.filter { with (Shipping.Dimensions) { Width.equal(Height) } }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db {
                    val results = Products.filter { with (Shipping.Dimensions) { Width.equal(Depth) } }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db {
                    val results = Products.filter { with (Shipping.Dimensions) { Width.notEqual(Depth) } }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db {
                    val results = Products.filter { with (Shipping.Dimensions) { Width.notEqual(Height) } }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            //

            on("filtering an abstract schema by gt expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Width.gt(Shipping.Dimensions.Depth) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by gt expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Depth.gt(Shipping.Dimensions.Width) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by lt expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Depth.lt(Shipping.Dimensions.Width) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by lt expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Width.lt(Shipping.Dimensions.Depth) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Width.ge(Shipping.Dimensions.Height) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Width.ge(Shipping.Dimensions.Depth) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Depth.ge(Shipping.Dimensions.Width) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Depth.le(Shipping.Dimensions.Width) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Width.le(Shipping.Dimensions.Height) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db {
                    val results = Products.filter { Shipping.Dimensions.Width.le(Shipping.Dimensions.Depth) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("getting one column by regex filter expression") {
                db {
                    val results = Albums.filter { Details.Title.matches("Love Supreme".toRegex()) }.toList()
                    it("returns correct values") {
                        validate(results)
                    }
                }
            }

            on("getting one column by regex filter expression") {
                db {
                    val results = Albums.filter { Details.Title.matches("Love Supremex".toRegex()) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("setting a new value to a string column on a non-abstract schema by id") {
                db {
                    Albums.select { Details.Title }.find(albumId!!).set("A Love Supreme. Original Recording Reissued")
                    val title = Albums.select { Details.Title }.get(albumId!!)
                    it("takes effect") {
                        assertEquals("A Love Supreme. Original Recording Reissued", title)
                    }
                }
            }

            on("setting a new value for a string column on a non-abstract schema by id") {
                db {
                    Albums.select { Details.Title }.find(albumId!!).set("A Love Supreme. Original Recording Reissued")
                    val title = Albums.select { Details.Title }.get(albumId!!)
                    it("takes effect") {
                        assertEquals("A Love Supreme. Original Recording Reissued", title)
                    }
                }
            }

            on("setting values for two integer columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings }.filter { SKU.equal("00e8da9b") }.set(1150, 50)
                    val (retail, savings)= Albums.select { Pricing.Retail + Pricing.Savings }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                    }
                }
            }

            on("setting values for three columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings + Pricing.List }.filter { SKU.equal("00e8da9b") }.set(1150, 50, 1250)
                    val (retail, savings, list)= Albums.select { Pricing.Retail + Pricing.Savings + Pricing.List }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                    }
                }
            }

            on("setting values for four columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width }.filter { SKU.equal("00e8da9b") }.set(1150, 50, 1250, 11)
                    val (retail, savings, list, width)= Albums.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                        assertEquals(11, width)
                    }
                }
            }

            on("setting values for five columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height }.filter { SKU.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13)
                    val (retail, savings, list, width, height)= Albums.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                        assertEquals(11, width)
                        assertEquals(13, height)
                    }
                }
            }

            on("setting values for six columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth }.filter { SKU.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2)
                    val (retail, savings, list, width, height, depth)= Albums.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                        assertEquals(11, width)
                        assertEquals(13, height)
                        assertEquals(2, depth)
                    }
                }
            }

            on("setting values for seven columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth + Shipping.Weight }.filter { SKU.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2, 7)
                    val (retail, savings, list, width, height, depth, weight)= Albums.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth + Shipping.Weight }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                        assertEquals(11, width)
                        assertEquals(13, height)
                        assertEquals(2, depth)
                        assertEquals(7, weight)
                    }
                }
            }

            on("setting values for eight columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth + Shipping.Weight  + Cost }.filter { SKU.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2, 7, 1.25)
                    val (retail, savings, list, width, height, depth, weight, cost)= Albums.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth + Shipping.Weight + Cost }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                        assertEquals(11, width)
                        assertEquals(13, height)
                        assertEquals(2, depth)
                        assertEquals(7, weight)
                        assertEquals(1.25, cost)
                    }
                }
            }

            on("setting values for nine columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth + Shipping.Weight + Cost  + Available }.filter { SKU.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2, 7, 1.25, false)
                    val (retail, savings, list, width, height, depth, weight, cost, available)= Albums.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth + Shipping.Weight + Cost + Available }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                        assertEquals(11, width)
                        assertEquals(13, height)
                        assertEquals(2, depth)
                        assertEquals(7, weight)
                        assertEquals(1.25, cost)
                        assertEquals(false, available)
                    }
                }
            }

            on("setting values for ten columns on an abstract schema by a filter expression") {
                db {
                    Products.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth + Shipping.Weight + Cost + Available + NullableDoubleWithValue }.filter { SKU.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2, 7, 1.25, false, 10.1)
                    val (retail, savings, list, width, height, depth, weight, cost, available, nullableDoubleWithValue) = Albums.select { Pricing.Retail + Pricing.Savings + Pricing.List + Shipping.Dimensions.Width + Shipping.Dimensions.Height + Shipping.Dimensions.Depth + Shipping.Weight + Cost + Available + NullableDoubleWithValue }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                        assertEquals(11, width)
                        assertEquals(13, height)
                        assertEquals(2, depth)
                        assertEquals(7, weight)
                        assertEquals(1.25, cost)
                        assertEquals(false, available)
                        assertEquals(10.1, nullableDoubleWithValue)
                    }
                }
            }

            on("setting a new value to a date column on a non-abstract schema by id") {
                db {
                    Albums.select { NullableDateNoValue }.find(albumId!!).set(LocalDate(2014, 3, 20))
                    val nullableDateNoValue = Albums.select { Details.Title }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(LocalDate(2014, 3, 20), nullableDateNoValue!!)
                    }
                }
            }

            on("adding a new element to a list column on a non-abstract schema by id") {
                db {
                    Albums.select { Details.Tracks }.find(albumId!!).add(Track("A Love Supreme, Part IV-Psalm", 400))
                    val tracks = Albums.select { Albums.Details.Tracks }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(4, tracks.size)
                        assertEquals("A Love Supreme, Part IV-Psalm", tracks[3].title)
                        assertEquals(400, tracks[3].duration)
                    }
                }
            }

            // TODO TODO TODO
/*
            on("getting range of values for a list column on a non-abstract schema by id") {
                db {
                    val tracks = Albums columns { Albums.Details.Tracks } at albumId!! range 1..2
                    it("takes effect") {
                        assertEquals(4, tracks.size)
                        assertEquals("A Love Supreme, Part IV-Psalm", tracks[3].title)
                        assertEquals(400, tracks[3].duration)
                    }
                }
            }
*/



            on("removing sn element from a collection column on a non-abstract schema by id") {
                db {
                    Albums.select { Details.Tracks }.find(albumId!!).delete { Duration.equal(100) }
                    val tracks = Albums.select { Albums.Details.Tracks }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(3, tracks.size)
                    }
                }
            }

            on("removing an element from a collection column on a non-abstract schema by a filter expression") {
                db {
                    Albums.select { Details.Tracks }.filter { SKU.equal("00e8da9b") }.delete { Duration.equal(200) }
                    val tracks = Albums.select { Albums.Details.Tracks } get albumId!!
                    it("takes effect") {
                        assertEquals(2, tracks.size)
                    }
                }
            }

            on("removing an element from a set column on a non-abstract schema by id") {
                db {
                    Albums.select { Details.Genre }.find(albumId!!).delete("General")
                    val genre = Albums.select { Albums.Details.Genre }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1, genre.size)
                    }
                }
            }

            on("deleting a document") {
                db {
                    Albums.delete { Id.equal(albumId!!) }
                    it("deletes the document from database") {
                        assert(Albums.filter { Id.equal(albumId!!) }.toList().isEmpty())
                    }
                }
            }
        }
    }
}

