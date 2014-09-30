package kotlinx.nosql.mongodb.test

import kotlin.test.assertEquals
import kotlinx.nosql.*
import kotlinx.nosql.mongodb.*
import org.joda.time.LocalDate
import org.jetbrains.spek.api.Spek

class MongoDBSpek : Spek() {
    open class ProductSchema<D, S : DocumentSchema<D>>(javaClass: Class<D>, discriminator: String) : DocumentSchema<D>("products",
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
        val setOfStrings = setOfString<S>("setOfStrings")

        val shipping = ShippingColumn<S>()
        val pricing = PricingColumn<S>()

        inner class ShippingColumn<S : DocumentSchema<D>>() : Column<Shipping, S>("shipping", javaClass()) {
            val weight = integer<S>("weight")
            val dimensions = DimensionsColumn<S>()
        }

        inner class DimensionsColumn<S : DocumentSchema<D>>() : Column<Dimensions, S>("dimensions", javaClass()) {
            val width = integer<S>("width")
            val height = integer<S>("height")
            val depth = integer<S>("depth")
        }

        inner class PricingColumn<T : DocumentSchema<D>>() : Column<Pricing, T>("pricing", javaClass()) {
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

    object Artists : DocumentSchema<Artist>("artists", javaClass()) {
        val name = string("name")
    }

    object Products : ProductSchema<Product, Products>(javaClass(), "") {
    }

    object Albums : ProductSchema<Album, Albums>(javaClass(), discriminator = "Audio Album") {
        val details = DetailsColumn()

        class DetailsColumn() : Column<Details, Albums>("details", javaClass()) {
            val title = string("title")
            val artistId = id("artistId", Artists)
            val artistIds = setOfId("artistIds", Artists)
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
                           val nullableDateNoValue: org.joda.time.LocalDate?, val nullableDateWithValue: LocalDate?,
                           val nullableDoubleNoValue: Double?, val nullableDoubleWithValue: Double?,
                           val setOfStrings: Set<String>, val shipping: Shipping, val pricing: Pricing) {
        val id: Id<String, Products>? = null // How to define id for implementation classes?
    }

    class Shipping(val weight: Int, val dimensions: Dimensions) {
    }

    class Dimensions(val width: Int, val height: Int, val depth: Int) {
    }

    class Pricing(val list: Int, val retail: Int, val savings: Int, val pctSavings: Int) {
    }

    class Album(sku: String, title: String, description: String, asin: String, available: Boolean,
                cost: Double, createdAtDate: org.joda.time.LocalDate,
                nullableBooleanNoValue: Boolean?, nullableBooleanWithValue: Boolean?,
                nullableDateNoValue: LocalDate?, nullableDateWithValue: LocalDate?,
                nullableDoubleNoValue: Double?, nullableDoubleWithValue: Double?,
                setOfStrings: Set<String>, shipping: Shipping, pricing: Pricing,
                val details: Details) : Product(sku, title, description, asin, available, cost, createdAtDate,
            nullableBooleanNoValue, nullableBooleanWithValue, nullableDateNoValue, nullableDateWithValue,
            nullableDoubleNoValue, nullableDoubleWithValue, setOfStrings, shipping, pricing) {
    }

    class Artist(val name: String) {
        val id: Id<String, Artists>? = null
    }

    class Details(val title: String, val artistId: Id<String, Artists>, val artistIds: Set<Id<String, Artists>>,
                  val genre: Set<String>, val tracks: List<Track>) {
    }

    class Track(val title: String, val duration: Int) {
    }

    {
        given("a polymorhpic schema") {
            var artistId: Id<String, Artists>? = null
            var artistId2: Id<String, Artists>? = null
            var albumId: Id<String, Albums>? = null

            val db = MongoDB(schemas = array(Artists, Products, Albums), initialization = CreateDrop(onCreate = {
                val arId: Id<String, Artists> = Artists.insert(Artist(name = "John Coltrane"))
                val arId2: Id<String, Artists> = Artists.insert(Artist(name = "Andrey Cheptsov"))
                assert(arId.value.length > 0)
                val aId = Albums.insert(Album(sku = "00e8da9b", title = "A Love Supreme", description = "by John Coltrane",
                        asin = "B0000A118M", available = true, cost = 1.23, createdAtDate = LocalDate(2014, 3, 8), nullableBooleanNoValue = null,
                        nullableBooleanWithValue = false, nullableDateNoValue = null, nullableDateWithValue = LocalDate(2014, 3, 7),
                        nullableDoubleNoValue = null, nullableDoubleWithValue = 1.24,
                        setOfStrings = setOf("Something"), shipping = Shipping(weight = 6, dimensions = Dimensions(10, 10, 1)),
                        pricing = Pricing(list = 1200, retail = 1100, savings = 100, pctSavings = 8),
                        details = Details(title = "A Love Supreme [Original Recording Reissued]",
                                artistId = arId, artistIds = setOf(arId, arId2),
                                genre = setOf("Jazz", "General"), tracks = listOf(Track("A Love Supreme Part I: Acknowledgement", 100),
                                Track("A Love Supreme Part II - Resolution", 200),
                                Track("A Love Supreme, Part III: Pursuance", 300)))))
                assert(aId.value.length > 0)
                albumId = aId
                artistId = arId
                artistId2 = arId2
            }))

            on("filtering a non-inherited schema") {
                val a = db.withSession {
                    val artists = Artists.find { name.equal("John Coltrane") }.toList()
                    it("should return a generated id for artist") {
                        assert(artists.size == 1)
                    }
                    "a"
                }
                kotlin.test.assertEquals("a", a)
            }

            fun validate(results: List<Product>) {
                assert(results.size == 1)
                assert(results[0] is Album)
                val album = results[0] as Album
                assertEquals("00e8da9b", results[0].sku)
                assertEquals(true, results[0].available)
                assertEquals(1.23, results[0].cost)
                kotlin.test.assertEquals(LocalDate(2014, 3, 8), results[0].createdAtDate)
                assert(results[0].nullableDateNoValue == null)
                assertEquals(LocalDate(2014, 3, 7), results[0].nullableDateWithValue)
                assert(results[0].nullableDoubleNoValue == null)
                assertEquals(1.24, results[0].nullableDoubleWithValue)
                assert(results[0].nullableBooleanNoValue == null)
                assertEquals(false, results[0].nullableBooleanWithValue)
                kotlin.test.assertEquals("A Love Supreme", results[0].title)
                assertEquals("by John Coltrane", results[0].description)
                assertEquals("B0000A118M", results[0].asin)
                assert(album.setOfStrings.contains("Something"))
                kotlin.test.assertEquals(6, results[0].shipping.weight)
                kotlin.test.assertEquals(10, results[0].shipping.dimensions.width)
                kotlin.test.assertEquals(10, results[0].shipping.dimensions.height)
                kotlin.test.assertEquals(1, results[0].shipping.dimensions.depth)
                assertEquals(1200, results[0].pricing.list)
                kotlin.test.assertEquals(1100, results[0].pricing.retail)
                assertEquals(100, results[0].pricing.savings)
                kotlin.test.assertEquals(8, results[0].pricing.pctSavings)
                kotlin.test.assertEquals("A Love Supreme [Original Recording Reissued]", album.details.title)
                assertEquals(artistId!!, album.details.artistId)
                assert(album.details.artistIds.size == 2)
                assert(album.details.artistIds.contains(artistId))
                assert(album.details.artistIds.contains(artistId2))
                assert(album.details.genre.size == 2)
                assert(album.details.genre.contains("Jazz"))
                assert(album.details.genre.contains("General"))
                assert(album.details.tracks.size == 3)
                assertEquals(album.details.tracks[0].title, "A Love Supreme Part I: Acknowledgement")
                assertEquals(album.details.tracks[0].duration, 100)
                assertEquals(album.details.tracks[1].title, "A Love Supreme Part II - Resolution")
                assertEquals(album.details.tracks[1].duration, 200)
                assertEquals(album.details.tracks[2].title, "A Love Supreme, Part III: Pursuance")
                kotlin.test.assertEquals(album.details.tracks[2].duration, 300)
            }

            /**
             * Remove Rxjava
             * Replace Observable with Iterable
             */

            on("filtering an abstract schema") {
                db.withSession {
                    val results = Products.find { (sku.equal("00e8da9b")).or(shipping.weight.equal(6)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering a non-abstract schema") {
                db.withSession {
                    val results: List<Album> = Albums.find { details.artistId.equal(artistId!!) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering a non-abstract schema drop take") {
                db.withSession {
                    val results = Products.find { (sku.equal("00e8da9b")).or(shipping.weight.equal(6)) }.skip(1).take(1).toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("getting all elements from a non-abstract schema") {
                db.withSession {
                    val results = Products.find().toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("getting a document by id") {
                db.withSession {
                    val album = Albums.find { id.equal(albumId!!) }.single()
                    it("should return a correct object") {
                        validate(listOf(album))
                    }
                }
            }

            on("getting one column by id") {
                db.withSession {
                    val title = Albums.find { id.equal(albumId!!) }.projection { details.title }.single()
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                    }
                }
            }

            on("getting two columns by id") {
                db.withSession {
                    val (title, pricing) = Albums.find { id.equal(albumId!!) }.projection { details.title + pricing }.single()
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
                    val (sku, title, pricing) = Albums.find { id.equal(albumId!!) }.projection { sku + details.title + pricing }.single()
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
                    val (sku, title, description, pricing) = Albums.find{ id.equal(albumId!!) }.projection { sku + title + description + pricing }.single()
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
                    val (sku, title, description, asin, pricing) = Albums.find{ id.equal(albumId!!) }.projection { sku + title + description + asin + pricing }.single()
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
                    val (sku, title, description, asin, list, retail) = Albums.find { id.equal(albumId!!) }. projection { sku + title + description + asin + pricing.list + pricing.retail }.single()
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
                    val (sku, title, description, asin, list, retail, savings) = Albums.find { id.equal(albumId!!) }.projection { sku + title + description + asin + pricing.list + pricing.retail + pricing.savings}.single()
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings) = Albums.find { id.equal(albumId!!) }.projection { sku + title + description + asin + pricing.list + pricing.retail + pricing.savings + pricing.pctSavings }.single()
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings, shipping) = Albums.find { id.equal(albumId!!) }.projection { sku + title + description + asin + pricing.list + pricing.retail + pricing.savings + pricing.pctSavings + shipping }.single()
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
                    val a = with (Albums) { sku + title + description + asin + pricing.list + pricing.retail + pricing.savings + pricing.pctSavings + shipping.weight + shipping.dimensions }
                    val (sku, title, description, asin, list, retail, savings, pctSavings, weight, dimensions) = Albums.find { id.equal(albumId!!) }.projection { a }.single()
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
                    val aId = Albums.find { id.equal(albumId!!) }.projection { details.artistId }.single()
                    it("returns correct values") {
                        assertEquals(artistId, aId)
                    }
                }
            }

            on("getting one column by filter expression") {
                db.withSession {
                    val title = Albums.find { sku.equal("00e8da9b") }.projection { details.title }.single()
                    it("returns correct values") {
                        assertEquals("A Love Supreme [Original Recording Reissued]", title)
                    }
                }
            }

            on("getting two columns by a filter expression") {
                db.withSession {
                    val (title, pricing) = Albums.find { sku.equal("00e8da9b") }.projection { details.title + pricing }.single()
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
                    val (sku, title, pricing) = Albums.find { sku.equal("00e8da9b") }.projection { sku + details.title + pricing }.single()
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
                    val (sku, title, description, pricing) = Products.find { sku.equal("00e8da9b") }.projection { sku + title + description + pricing }.single()
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
                    val (sku, title, description, asin, pricing) = Products.find { sku.equal("00e8da9b") }.projection { sku + title + description + asin + pricing }.single()
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
                    val (sku, title, description, asin, list, retail) = Products.find { sku.equal("00e8da9b") }.projection { sku + title + description + asin + pricing.list + pricing.retail }.single()
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
                    val (sku, title, description, asin, list, retail, savings) = Products.find { sku.equal("00e8da9b") }.projection { sku + title + description + asin + pricing.list + pricing.retail + pricing.savings }.single()
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings) = Products.find { sku.equal("00e8da9b") }.projection { sku + title + description + asin + pricing.list + pricing.retail + pricing.savings + pricing.pctSavings }.single()
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings, shipping) = Products.find { sku.equal("00e8da9b") }.projection { sku + title + description + asin + pricing.list + pricing.retail + pricing.savings + pricing.pctSavings + shipping}.single()
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
                    val (sku, title, description, asin, list, retail, savings, pctSavings, weight, dimensions) = Products.find { sku.equal("00e8da9b") }.projection { sku + title + description + asin + pricing.list + pricing.retail + pricing.savings + pricing.pctSavings + shipping.weight + shipping.dimensions }.single()
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
                    val results = Products.find { text("Love") }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by search expression") {
                db.withSession {
                    val results = Products.find { text("Love") and shipping.weight.equal(16) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by search expression (returns nothing)") {
                db.withSession {
                    val results = Products.find { text("Love1") }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.equal(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.equal(7) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by notEqual expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.notEqual(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by notEqual expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.notEqual(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by gt expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.gt(5) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by gt expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.gt(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by lt expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.lt(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by lt expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.lt(6) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.ge(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.ge(5) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.ge(7) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.le(6) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.le(7) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.le(5) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by mb expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.memberOf(array(5, 6)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by mb expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.memberOf(array(5, 7)) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by nm expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.notMemberOf(array(5, 7)) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by nm expression") {
                db.withSession {
                    val results = Products.find { shipping.weight.notMemberOf(array(5, 6)) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db.withSession {
                    val results = Products.find { with (shipping.dimensions) { width.equal(height) } }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db.withSession {
                    val results = Products.find { with (shipping.dimensions) { width.equal(depth) } }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db.withSession {
                    val results = Products.find { with (shipping.dimensions) { width.notEqual(depth) } }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by equal expression - compare to a column") {
                db.withSession {
                    val results = Products.find { with (shipping.dimensions) { width.notEqual(height) } }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            //

            on("filtering an abstract schema by gt expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.width.gt(shipping.dimensions.depth) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by gt expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.depth.gt(shipping.dimensions.width) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by lt expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.depth.lt(shipping.dimensions.width) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by lt expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.width.lt(shipping.dimensions.depth) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.width.ge(shipping.dimensions.height) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.width.ge(shipping.dimensions.depth) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by ge expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.depth.ge(shipping.dimensions.width) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.depth.le(shipping.dimensions.width) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.width.le(shipping.dimensions.height) }.toList()
                    it("should return a correct object") {
                        validate(results)
                    }
                }
            }

            on("filtering an abstract schema by le expression - compare to a column") {
                db.withSession {
                    val results = Products.find { shipping.dimensions.width.le(shipping.dimensions.depth) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("getting one column by regex filter expression") {
                db.withSession {
                    val results = Albums.find { details.title.matches("Love Supreme".toRegex()) }.toList()
                    it("returns correct values") {
                        validate(results)
                    }
                }
            }

            on("getting one column by regex filter expression") {
                db.withSession {
                    val results = Albums.find { details.title.matches("Love Supremex".toRegex()) }.toList()
                    it("should return nothing") {
                        assert(results.isEmpty())
                    }
                }
            }

            on("setting a new value to a string column on a non-abstract schema by id") {
                db.withSession {
                    Albums.find { id.equal(albumId!!) }.projection { details.title }.update("A Love Supreme. Original Recording Reissued")
                    val title = Albums.find { id.equal(albumId!!) }.projection { details.title }.single()!!
                    it("takes effect") {
                        assertEquals("A Love Supreme. Original Recording Reissued", title)
                    }
                }
            }

            on("setting a new value for a string column on a non-abstract schema by id") {
                db.withSession {
                    Albums.find { id.equal(albumId!!) }.projection { details.title }.update("A Love Supreme. Original Recording Reissued")
                    val title = Albums.find { id.equal(albumId!!) }.projection { details.title }.single()
                    it("takes effect") {
                        assertEquals("A Love Supreme. Original Recording Reissued", title)
                    }
                }
            }

            on("setting values for two integer columns on an abstract schema by a filter expression") {
                db.withSession {
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings }.update(1150, 50)
                    val (retail, savings) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings }.single()
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                    }
                }
            }

            on("setting values for three columns on an abstract schema by a filter expression") {
                db.withSession {
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings + pricing.list }.update(1150, 50, 1250)
                    val (retail, savings, list) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings + pricing.list }.single()
                    it("takes effect") {
                        assertEquals(1150, retail)
                        assertEquals(50, savings)
                        assertEquals(1250, list)
                    }
                }
            }

            on("setting values for four columns on an abstract schema by a filter expression") {
                db.withSession {
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width }.update(1150, 50, 1250, 11)
                    val (retail, savings, list, width) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width }.single()
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
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height }.update(1150, 50, 1250, 11, 13)
                    val (retail, savings, list, width, height) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height }.single()
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
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth }.update(1150, 50, 1250, 11, 13, 2)
                    val (retail, savings, list, width, height, depth) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth }.single()
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
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight }.update(1150, 50, 1250, 11, 13, 2, 7)
                    val (retail, savings, list, width, height, depth, weight) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight }.single()
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
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost }.update(1150, 50, 1250, 11, 13, 2, 7, 1.25)
                    val (retail, savings, list, width, height, depth, weight, cost) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost }.single()
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
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost + available }.update(1150, 50, 1250, 11, 13, 2, 7, 1.25, false)
                    val (retail, savings, list, width, height, depth, weight, cost, available) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost + available }.single()
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
                    Products.find { sku.equal("00e8da9b") }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost + available + nullableDoubleWithValue }.update(1150, 50, 1250, 11, 13, 2, 7, 1.25, false, 10.1)
                    val (retail, savings, list, width, height, depth, weight, cost, available, nullableDoubleWithValue) = Albums.find { id.equal(albumId!!) }.projection { pricing.retail + pricing.savings + pricing.list + shipping.dimensions.width + shipping.dimensions.height + shipping.dimensions.depth + shipping.weight + cost + available + nullableDoubleWithValue }.single()
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
                    Albums.find { id.equal(albumId!!) }.projection { nullableDateNoValue }.update(LocalDate(2014, 3, 20))
                    // TODO: single is nullable here
                    val nullableDateNoValue = Albums.find { id.equal(albumId!!) }.projection { nullableDateNoValue }.single()
                    it("takes effect") {
                        assertEquals(LocalDate(2014, 3, 20), nullableDateNoValue!!)
                    }
                }
            }

            on("adding a new element to a list column on a non-abstract schema by id") {
                db.withSession {
                    Albums.find { id.equal(albumId!!) }.projection { details.tracks }.add(Track("A Love Supreme, Part IV-Psalm", 400))
                    val tracks = Albums.find { id.equal(albumId!!) }.projection { Albums.details.tracks }.single()!!
                    it("takes effect") {
                        assertEquals(4, tracks.size)
                        assertEquals("A Love Supreme, Part IV-Psalm", tracks[3].title)
                        assertEquals(400, tracks[3].duration)
                    }
                }
            }

            on("removing sn element from a collection column on a non-abstract schema by id") {
                db.withSession {
                    Albums.find { id.equal(albumId!!) }.projection { details.tracks }.remove { duration.equal(100) }
                    val tracks = Albums.find { id.equal(albumId!!) }.projection { details.tracks }.single()
                    it("takes effect") {
                        assertEquals(3, tracks.size)
                    }
                }
            }

            on("removing an element from a collection column on a non-abstract schema by a filter expression") {
                db.withSession {
                    Albums.find { sku.equal("00e8da9b") }.projection { details.tracks }.remove { duration.equal(200) }
                    val tracks = Albums.find { id.equal(albumId!!) }.projection { Albums.details.tracks }.single()
                    it("takes effect") {
                        assertEquals(2, tracks.size)
                    }
                }
            }

            on("removing an element from a set column on a non-abstract schema by id") {
                db.withSession {
                    Albums.find { id.equal(albumId!!) }.projection { details.genre }.remove("General")
                    val genre = Albums.find { id.equal(albumId!!) }.projection { Albums.details.genre }.single()
                    it("takes effect") {
                        assertEquals(1, genre.size)
                    }
                }
            }

            on("deleting a document") {
                db.withSession {
                    Albums.find { id.equal(albumId!!) }.remove()
                    val results =  Albums.find { id.equal(albumId!!) }.toList()
                    it("deletes the document from database") {
                        assert(results.isEmpty())
                    }
                }
            }
        }
    }
}

