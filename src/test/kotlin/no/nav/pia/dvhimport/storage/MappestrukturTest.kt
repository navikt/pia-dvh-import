package no.nav.pia.dvhimport.storage

import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal
import kotlin.test.Test

class MappestrukturTest {
    @Test
    fun `utleder siste årstall og kvartal basert på mappe struktur`() {
        val mappestruktur = Mappestruktur(
            publiseringsÅr = "2024",
            sistePubliserteKvartal = "K2",
        )

        mappestruktur.sistePubliserteKvartal shouldBe "K2"
        mappestruktur.publiseringsÅr shouldBe "2024"
        mappestruktur.gjeldendeÅrstallOgKvartal() shouldBe ÅrstallOgKvartal(årstall = 2024, kvartal = 2)
    }
}
