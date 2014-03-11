package org.spek

import java.util.ArrayList
import org.junit.Test

// TODO TODO TODO Migrate to Spek when it's fixed
open class Spek() {
    val statements = ArrayList<GivenBody.() -> Unit>()

    Test fun test() {
        val givenBody = GivenBody()
        for (statement in statements) {
            givenBody.statement()
        }
    }

    fun given(name: String, statement: GivenBody.() -> Unit) {
        statements.add(statement)
    }

    class GivenBody {
        fun on(name: String, statement: OnBody.() -> Unit) {
            OnBody().statement()
        }
    }

    class OnBody {
        fun it(name: String, statement: () -> Unit) {
            statement()
        }
    }
}