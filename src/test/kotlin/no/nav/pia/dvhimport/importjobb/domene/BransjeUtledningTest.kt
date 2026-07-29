package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.definisjoner.bransjer.Bransje
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.ImportService
import java.math.BigDecimal
import kotlin.test.Test

class BransjeUtledningTest {

    private fun næringskode(
        kode: String,
        tapteDagsverk: String,
        muligeDagsverk: String,
        tapteDagsverkGradert: String,
        antallPersoner: Int,
        varighet: List<TapteDagsverkPerVarighetDto>,
    ) = NæringskodeSykefraværsstatistikkDto(
        næringskode = kode,
        årstall = 2024,
        kvartal = 2,
        prosent = BigDecimal("0"),
        tapteDagsverk = BigDecimal(tapteDagsverk),
        muligeDagsverk = BigDecimal(muligeDagsverk),
        tapteDagsverkGradert = BigDecimal(tapteDagsverkGradert),
        tapteDagsverkPerVarighet = varighet,
        antallPersoner = antallPersoner,
    )

    @Test
    fun `utleder bransje ved å aggregere flere næringskoder`() {
        val næringskoder = listOf(
            næringskode(
                kode = "87101",
                tapteDagsverk = "100",
                muligeDagsverk = "1000",
                tapteDagsverkGradert = "10",
                antallPersoner = 50,
                varighet = listOf(
                    TapteDagsverkPerVarighetDto("A", BigDecimal("1")),
                    TapteDagsverkPerVarighetDto("D", BigDecimal("4")),
                ),
            ),
            næringskode(
                kode = "87102",
                tapteDagsverk = "100",
                muligeDagsverk = "1000",
                tapteDagsverkGradert = "10",
                antallPersoner = 50,
                varighet = listOf(
                    TapteDagsverkPerVarighetDto("A", BigDecimal("2")),
                    TapteDagsverkPerVarighetDto("D", BigDecimal("6")),
                ),
            ),
        )

        val bransje = with(ImportService.Companion) {
            næringskoder.utleddBransjeStatistikk(årstall = 2024, kvartal = 2, bransje = Bransje.SYKEHJEM)
        }!!

        bransje.bransje shouldBe Bransje.SYKEHJEM.navn
        bransje.årstall shouldBe 2024
        bransje.kvartal shouldBe 2
        // Aggregering: summer over næringskodene
        bransje.tapteDagsverk.compareTo(BigDecimal("200")) shouldBe 0
        bransje.muligeDagsverk.compareTo(BigDecimal("2000")) shouldBe 0
        bransje.tapteDagsverkGradert.compareTo(BigDecimal("20")) shouldBe 0
        bransje.antallPersoner shouldBe 100
        // Prosent regnes på nytt fra aggregerte tall: 200 / 2000 * 100 = 10.0
        bransje.prosent.compareTo(BigDecimal("10.0")) shouldBe 0
        // Varighet aggregeres per varighetstype, sortert
        bransje.tapteDagsverkPerVarighet.size shouldBe 2
        bransje.tapteDagsverkPerVarighet[0].varighet shouldBe "A"
        bransje.tapteDagsverkPerVarighet[0].tapteDagsverk!!.compareTo(BigDecimal("3")) shouldBe 0
        bransje.tapteDagsverkPerVarighet[1].varighet shouldBe "D"
        bransje.tapteDagsverkPerVarighet[1].tapteDagsverk!!.compareTo(BigDecimal("10")) shouldBe 0
    }

    @Test
    fun `tom liste næringskoder gir null bransje`() {
        val resultat = with(ImportService.Companion) {
            emptyList<NæringskodeSykefraværsstatistikkDto>().utleddBransjeStatistikk(
                årstall = 2024,
                kvartal = 2,
                bransje = Bransje.SYKEHJEM,
            )
        }
        resultat.shouldBeNull()
    }
}
