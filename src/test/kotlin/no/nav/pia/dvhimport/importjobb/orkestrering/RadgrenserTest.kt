package no.nav.pia.dvhimport.importjobb.orkestrering

import io.kotest.matchers.shouldBe
import kotlin.test.Test

class RadgrenserTest {

    @Test
    fun `prod-profilen har prod-grenser for virksomhet`() {
        val grenser = Radgrenser.forCluster("prod-gcp")
        grenser.forSteg(ImportSteg.IMPORT_VIRKSOMHET) shouldBe Radgrense(300_000, 500_000)
        grenser.forSteg(ImportSteg.IMPORT_VIRKSOMHET_METADATA) shouldBe Radgrense(300_000, 500_000)
        grenser.forSteg(ImportSteg.IMPORT_LAND) shouldBe Radgrense(1, 1)
    }

    @Test
    fun `dev-profilen har lavere virksomhetsgrense men samme små-grenser`() {
        val grenser = Radgrenser.forCluster("dev-gcp")
        grenser.forSteg(ImportSteg.IMPORT_VIRKSOMHET) shouldBe Radgrense(1_000, 3_000)
        grenser.forSteg(ImportSteg.IMPORT_VIRKSOMHET_METADATA) shouldBe Radgrense(1_000, 3_000)
        grenser.forSteg(ImportSteg.IMPORT_SEKTOR) shouldBe Radgrense(3, 5)
        grenser.forSteg(ImportSteg.IMPORT_NARINGSKODE) shouldBe Radgrense(500, 1500)
    }

    @Test
    fun `ukjent cluster gir lokal-profil uten grenser`() {
        val grenser = Radgrenser.forCluster("lokal")
        ImportSteg.entries.forEach { steg ->
            grenser.forSteg(steg) shouldBe Radgrense(0, Int.MAX_VALUE)
        }
    }

    @Test
    fun `inneholder sjekker intervallet inklusivt`() {
        val grense = Radgrense(3, 5)
        grense.inneholder(2) shouldBe false
        grense.inneholder(3) shouldBe true
        grense.inneholder(5) shouldBe true
        grense.inneholder(6) shouldBe false
    }
}
