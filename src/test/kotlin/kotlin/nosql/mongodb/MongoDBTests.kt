package kotlin.nosql.mongodb

import org.junit.Test
import kotlin.nosql.mongodb.Product.Shipping

open class Schema<C>(val name: String, val javaClass: Class<C>) {
    {
        println(javaClass.getName())
    }
}

fun <T:Schema<*>> T.string(name: String): Column<T, String> {
    return Column<T, String>(name, javaClass())
}

open class Column<T:Schema<*>, C>(val name: String, val javaClass: Class<C>, val nullable: Boolean = false) {
    {
        println(javaClass.getName())
    }

    fun <B> plus(b: Column<T, B>): Template2<T, C, B> {
        return Template2<T, C, B>(this, b)
    }
}

object Products : Schema<Product>("products", javaClass()) {
    val sku = string("sku")

    class ShippingColumn() : Column<Products, Product.Shipping>("shipping", javaClass()) {
        val weight = string("weight")
    }

    val shipping = ShippingColumn()
}

trait Product {
    val sku: String
    val shipping: Shipping

    trait Shipping {
        val weight: String
    }
}

fun <T:Schema<*>, A, B> T.columns(st: T.() -> Template2<T, A, B>): Template2<T, A, B> {
    return st()
}

fun <T: Schema<C>, C> T.forEach(st: (C) -> Unit) {

}

/*fun <T: Schema<C>, C> T.get(op: T.() -> Op): C {
    return null as C
}*/

class Template2<T:Schema<*>, A, B>(a: Column<T, A>, b: Column<T, B>) {
    fun forEach(st: (a: A, b: B) -> Unit) {
        st("sku 1" as A, object: Shipping {
            override val weight: String = "137kg"
        } as B)
    }
}

class MongoDBTests {
    Test
    fun test() {
        Products columns { sku + shipping } forEach { sku, shipping ->
            println("sku: $sku")
            println("shipping's weight: ${shipping.weight}")
        }

        /*val product = Products get { sku eq "sku" }
        println(product)*/

        //Products insert { Product() }
    }
}