package kotlin.nosql.mongodb

import org.junit.Test
import kotlin.nosql.*

object Products : DocumentSchema<String, Product>("products", javaClass(), stringPK("sku")) {
    val sku = string("sku")

    // TODO
    class ShippingColumn() : Column<Product.Shipping, Products>(this, "shipping", javaClass()) {
        val weight = string("sku")
    }

    val shipping = ShippingColumn()
}

class Product(val sku: String, val shipping: Product.Shipping) {
    class Shipping(val weight: String) {
    }
}

class MongoDBTests {
    Test
    fun test() {
        /*val db:

        Products columns { sku + shipping } filter { sku eq "" } forEach { sku, shipping ->
            println("sku: $sku")
            println("shipping's weight: ${shipping.weight}")
        }
*/
        /*val product = Products get { sku eq "sku" }
        println(product)*/

        //Products insert { Product() }
    }
}