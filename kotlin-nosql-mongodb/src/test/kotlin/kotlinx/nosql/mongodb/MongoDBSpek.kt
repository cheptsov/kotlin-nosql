package kotlinx.nosql.mongodb

import org.spek.Spek
import kotlin.test.assertEquals
import kotlinx.nosql.*
import org.joda.time.LocalDate

class MongoDBSpek : Spek() {
    open class ProductSchema<D, S : Schema<D>>(javaClass: Class<D>, discriminator: String) : Schema<D>("products",
            javaClass, discriminator = Discriminator(string("type"), discriminator)) {
        val sku = string<S>("sku")
        val title = string<S>("title")
        val description = string<S>("description")
        val asin = string<S>("asin")
        val available = boolean<S>("available")
        val createdAtDate = date<S>("createdAtDate")
        val nullableBooleanNoValue = nullableBoolean<S>("nullableBooleanNoValue")
        val nullableBooleanWithValue = nullableBoolean<S>("nullableBooleanWithValue")
        val nullableDateNoValue = nullableDate<S>("nullableDateNoValue")
        val nullableDateWithValue = nullableDate<S>("nullableDateWithValue")
        val cost = double<S>("cost")
        val nullableDoubleNoValue = nullableDouble<S>("nullableDoubleNoValue")
        val nullableDoubleWithValue = nullableDouble<S>("nullableDoubleWithValue")

        val shipping = ShippingColumn<S>()
        val pricing = PricingColumn<S>()

        inner class ShippingColumn<S : Schema<D>>() : Column<Shipping, S>("shipping", javaClass()) {
            val weight = integer<S>("weight")
            val dimensions = DimensionsColumn<S>()
        }

        inner class DimensionsColumn<S : Schema<D>>() : Column<Dimensions, S>("dimensions", javaClass()) {
            val width = integer<S>("width")
            val height = integer<S>("height")
            val depth = integer<S>("depth")
        }

        inner class PricingColumn<T : Schema<D>>() : Column<Pricing, T>("pricing", javaClass()) {
            val list = integer<T>("list")
            val retail = integer<T>("retail")
            val savings = integer<T>("savings")
            val pctSavings = integer<T>("pct_savings")
        }

        {
            ensureIndex(text = array(title, description))
            ensureIndex(name = "asinIndex", unique = true, ascending = array(asin))
        }
    }

    object Artists : Schema<Artist>("artists", javaClass()) {
        val name = string("name")
    }

    object Products : ProductSchema<Product, Products>(javaClass(), "") {
    }

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

    abstract class Product(val sku: String, val title: String, val description: String,
                           val asin: String, val available: Boolean, val cost: Double,
                           val createdAtDate: LocalDate, val nullableBooleanNoValue: Boolean?,
                           val nullableBooleanWithValue: Boolean?,
                           val nullableDateNoValue: LocalDate?, val nullableDateWithValue: LocalDate?,
                           val nullableDoubleNoValue: Double?, val nullableDoubleWithValue: Double?,
                           val shipping: Shipping, val pricing: Pricing) {
        val id: Id<String, Products>? = null // How to define id for implementation classes?
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
            val db = MongoDB(schemas = array(Artists, Products, Albums), initialization = CreateDrop())

            db.withSession {
                array(Products, Artists).forEach { it.drop() }
            }
            var artistId: Id<String, Artists>? = null
            var albumId: Id<String, Albums>? = null

            on("inserting a document") {
                db.withSession {
                    val arId: Id<String, Artists> = Artists.insert(Artist(name = "John Coltrane"))
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
                db.withSession {
                    val artists = Artists.findAll { name.equal("John Coltrane") }.toList()
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
                db.withSession {
                    val results = Products.findAll { (sku.equal("00e8da9b")).or(shipping.weight.equal(6)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering a non-abstract schema") {
                db.withSession {
                    val results: List<Album> = Albums.findAll { details.artistId.equal(artistId!!) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering a non-abstract schema drop take") {
                db.withSession {
                    val results = Products.findAll { (sku.equal("00e8da9b")).or(shipping.weight.equal(6)) }.drop(1).take(1).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("getting all elements from a non-abstract schema") {
                db.withSession {
                    val results = Products.findAll().toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("getting a document by id") {
                db.withSession {
                    val album = Albums.get(albumId!!)
                    it("should return a correct object") {
                        validate(listOf(album))
                    }
                }
            }

            on("getting one column by id") {
                db.withSession {
                    val title = Albums.select { details.title }.get(albumId!!)
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                    }
                }
            }

            on("getting two columns by id") {
                db.withSession {
                    val (title, pricing) = Albums.select { details.title + pricing }.get(albumId!!)
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
                db.withSession {
                    val (sku, title, pricing) = Albums.select { sku + details.title + pricing }.get(albumId!!)
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
                db.withSession {
                    val (sku, title, description, pricing) = Albums.select { sku + title + description + pricing }.get(albumId!!)
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
                db.withSession {
                    val (sku, title, description, asin, pricing) = Albums.select { sku + title + description + asin + pricing }.get(albumId!!)
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
                db.withSession {
                    val (sku, title, description, asin, list, retail) = Albums.select {
                        sku + title +
                        description + asin + pricing.list + pricing.retail
                    }.get(albumId!!)
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
                db.withSession {
                    val (sku, title, description, asin, list, retail, savings) = Albums.select {
                        sku + title +
                        description + asin + pricing.list + pricing.retail + pricing.savings
                    }.get(albumId!!)
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
                db.withSession {
                    val (sku, title, description, asin, list, retail, savings, pctSavings) = Albums.select {
                        sku + title + description + asin + pricing.list + pricing.retail + pricing.savings +
                        pricing.pctSavings
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
                db.withSession {
                    val (sku, title, description, asin, list, retail, savings, pctSavings, shipping) = Albums.select {
                        sku + title + description + asin + pricing.list + pricing.retail + pricing.savings +
                        pricing.pctSavings + shipping
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
                db.withSession {
                    val (sku, title, description, asin, list, retail, savings, pctSavings, weight, dimensions) = Albums.select {
                        sku + title + description + asin + pricing.list + pricing.retail + pricing.savings +
                        pricing.pctSavings + shipping.weight + shipping.dimensions
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

            on("getting one id-column by another id") {
                db.withSession {
                    val aId = Albums.select { details.artistId }.get(albumId!!)
                    it("returns correct values") {
                        assertEquals(artistId, aId)
                    }
                }
            }

            on("getting one column by filter expression") {
                db.withSession {
                    val title = Albums.select { details.title }.findAll { sku.equal("00e8da9b") }.first()
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                    }
                }
            }

            on("getting two columns by a filter expression") {
                db.withSession {
                    val (title, pricing) = Albums.select { details.title + pricing }.findAll { sku.equal("00e8da9b") }.first()
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
                db.withSession {
                    val (sku, title, pricing) = Albums.select { sku + details.title + pricing }.findAll { sku.equal("00e8da9b") }.first()
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
                db.withSession {
                    val (sku, title, description, pricing) = Products.select { sku + title + description + pricing }.findAll { sku.equal("00e8da9b") }.first()
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
                db.withSession {
                    val (sku, title, description, asin, pricing) = Products.select { sku + title + description + asin + pricing }.findAll { sku.equal("00e8da9b") }.first()
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
                db.withSession {
                    val (sku, title, description, asin, list, retail) = Products.select {
                        sku + title +
                        description + asin + pricing.list + pricing.retail
                    }.findAll { sku.equal("00e8da9b") }.first()
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
                db.withSession {
                    val (sku, title, description, asin, list, retail, savings) = Products.select {
                        sku + title +
                        description + asin + pricing.list + pricing.retail + pricing.savings
                    }.findAll { sku.equal("00e8da9b") }.first()
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
                db.withSession {
                    val (sku, title, description, asin, list, retail, savings, pctSavings) = Products.select {
                        sku + title + description + asin + pricing.list + pricing.retail + pricing.savings +
                        pricing.pctSavings
                    }.findAll { sku.equal("00e8da9b") }.first()
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
                db.withSession {
                    val (sku, title, description, asin, list, retail, savings, pctSavings, shipping) = Products.select {
                        sku + title + description + asin + pricing.list + pricing.retail + pricing.savings +
                        pricing.pctSavings + shipping
                    }.findAll { sku.equal("00e8da9b") }.first()
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
                db.withSession {
                    val (sku, title, description, asin, list, retail, savings, pctSavings, weight, dimensions) = Products.select {
                        sku + title + description + asin + pricing.list + pricing.retail + pricing.savings +
                        pricing.pctSavings + shipping.weight + shipping.dimensions
                    }.findAll { sku.equal("00e8da9b") }.first()
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

            on("filtering an abstract schema by search expression") {
                db.withSession {
                    val results = Products.findAll { search("Love") }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by search expression (returns nothing)") {
                db.withSession {
                    val results = Products.findAll { search("Love1") }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.equal(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.equal(7) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by notEqual expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.notEqual(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by notEqual expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.notEqual(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by gt expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.gt(5) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by gt expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.gt(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by lt expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.lt(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by lt expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.lt(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.ge(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.ge(5) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.ge(7) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.le(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.le(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.le(5) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by mb expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.memberOf(array(5, 6)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by mb expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.memberOf(array(5, 7)) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by nm expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.notMemberOf(array(5, 7)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by nm expression") {
                db.withSession {
                    val results = Products.findAll { shipping.weight.notMemberOf(array(5, 6)) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { with (shipping.dimensions) { width.equal(height) } }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { with (shipping.dimensions) { width.equal(depth) } }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { with (shipping.dimensions) { width.notEqual(depth) } }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { with (shipping.dimensions) { width.notEqual(height) } }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            //

            on("filtering an abstract schema by gt expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.width.gt(shipping.dimensions.depth) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by gt expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.depth.gt(shipping.dimensions.width) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by lt expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.depth.lt(shipping.dimensions.width) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by lt expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.width.lt(shipping.dimensions.depth) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.width.ge(shipping.dimensions.height) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.width.ge(shipping.dimensions.depth) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.depth.ge(shipping.dimensions.width) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.depth.le(shipping.dimensions.width) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.width.le(shipping.dimensions.height) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db.withSession {
                    val results = Products.findAll { shipping.dimensions.width.le(shipping.dimensions.depth) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("getting one column by regex filter expression") {
                db.withSession {
                    val results = Albums.findAll { details.title.matches("Love Supreme".toRegex()) }.toList()
                    it("returns correct values") {
                        validate(results)
                    }
                }
            }

            on("getting one column by regex filter expression") {
                db.withSession {
                    val results = Albums.findAll { details.title.matches("Love Supremex".toRegex()) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("setting a new value to a string column on a non-abstract schema by id") {
                db.withSession {
                    Albums.select { details.title }.find(albumId!!).set("A Love Supreme. Original Recording Reissued")
                    val title = Albums.select { details.title }.get(albumId!!)
                    it("takes effect") {
                        assertEquals("A Love Supreme. Original Recording Reissued", title)
                    }
                }
            }

            on("setting a new value for a string column on a non-abstract schema by id") {
                db.withSession {
                    Albums.select { details.title }.find(albumId!!).set("A Love Supreme. Original Recording Reissued")
                    val title = Albums.select { details.title }.get(albumId!!)
                    it("takes effect") {
                        assertEquals("A Love Supreme. Original Recording Reissued", title)
                    }
                }
            }

            on("setting values for two integer columns on an abstract schema by a filter expression") {
                db.withSession {
                    Products.select { pricing.retail + pricing.savings }.findAll { sku.equal("00e8da9b") }.set(1150, 50)
                    val (retail, savings) = Albums.select { pricing.retail + pricing.savings }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                    }
                }
            }

            on("setting values for three columns on an abstract schema by a filter expression") {
                db.withSession {
                    Products.select { pricing.retail + pricing.savings + pricing.list }.findAll { sku.equal("00e8da9b") }.set(1150, 50, 1250)
                    val (retail, savings, list) = Albums.select { pricing.retail + pricing.savings + pricing.list }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                    }
                }
            }

            on("setting values for four columns on an abstract schema by a filter expression") {
                db.withSession {
                    Products.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width }.findAll { sku.equal("00e8da9b") }.set(1150, 50, 1250, 11)
                    val (retail, savings, list, width) = Albums.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                        assertEquals(11, width)
                    }
                }
            }

            on("setting values for five columns on an abstract schema by a filter expression") {
                db.withSession {
                    Products.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height }.findAll { sku.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13)
                    val (retail, savings, list, width, height) = Albums.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height }.get(albumId!!)
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
                db.withSession {
                    Products.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth }.findAll { sku.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2)
                    val (retail, savings, list, width, height, depth) = Albums.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth }.get(albumId!!)
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
                db.withSession {
                    Products.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight }.findAll { sku.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2, 7)
                    val (retail, savings, list, width, height, depth, weight) = Albums.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight }.get(albumId!!)
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
                db.withSession {
                    Products.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost }.findAll { sku.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2, 7, 1.25)
                    val (retail, savings, list, width, height, depth, weight, cost) = Albums.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost }.get(albumId!!)
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
                db.withSession {
                    Products.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost + available }.findAll { sku.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2, 7, 1.25, false)
                    val (retail, savings, list, width, height, depth, weight, cost, available) = Albums.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost + available }.get(albumId!!)
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
                db.withSession {
                    Products.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost + available + nullableDoubleWithValue }.findAll { sku.equal("00e8da9b") }.set(1150, 50, 1250, 11, 13, 2, 7, 1.25, false, 10.1)
                    val (retail, savings, list, width, height, depth, weight, cost, available, nullableDoubleWithValue) = Albums.select { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost + available + nullableDoubleWithValue }.get(albumId!!)
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
                db.withSession {
                    Albums.select { nullableDateNoValue }.find(albumId!!).set(LocalDate(2014, 3, 20))
                    val nullableDateNoValue = Albums.select { nullableDateNoValue }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(LocalDate(2014, 3, 20), nullableDateNoValue!!)
                    }
                }
            }

            on("adding a new element to a list column on a non-abstract schema by id") {
                db.withSession {
                    Albums.select { details.tracks }.find(albumId!!).add(Track("A Love Supreme, Part IV-Psalm", 400))
                    val tracks = Albums.select { Albums.details.tracks }.get(albumId!!)
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
                            db.withSession {
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
                db.withSession {
                    Albums.select { details.tracks }.find(albumId!!).delete { duration.equal(100) }
                    val tracks = Albums.select { Albums.details.tracks }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(3, tracks.size)
                    }
                }
            }

            on("removing an element from a collection column on a non-abstract schema by a filter expression") {
                db.withSession {
                    Albums.select { details.tracks }.findAll { sku.equal("00e8da9b") }.delete { duration.equal(200) }
                    val tracks = Albums.select { Albums.details.tracks } get albumId!!
                    it("takes effect") {
                        assertEquals(2, tracks.size)
                    }
                }
            }

            on("removing an element from a set column on a non-abstract schema by id") {
                db.withSession {
                    Albums.select { details.genre }.find(albumId!!).delete("General")
                    val genre = Albums.select { Albums.details.genre }.get(albumId!!)
                    it("takes effect") {
                        assertEquals(1, genre.size)
                    }
                }
            }

            on("deleting a document") {
                db.withSession {
                    Albums.delete { id.equal(albumId!!) }
                    it("deletes the document from database") {
                        assert(Albums.findAll { id.equal(albumId!!) }.toList().isEmpty())
                    }
                }
            }
        }
    }
}

