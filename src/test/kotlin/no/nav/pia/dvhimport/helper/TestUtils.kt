package no.nav.pia.dvhimport.helper

import io.kotest.matchers.shouldBe
import java.math.BigDecimal

class TestUtils {
    companion object {
        infix fun BigDecimal?.bigDecimalShouldBe(expected: Double) = this?.toDouble()?.shouldBe(expected) ?: false
    }
}
