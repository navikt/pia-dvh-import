package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.domene.StatistikkImportService.Companion.beregnSykefraværsprosentForLand
import java.math.BigDecimal
import kotlin.test.Test


class StatistikkImportServiceTest{
    @Test
    fun `kan regne ut sykefraværsprosent`() {
        val statistikk: List<SykefraværsstatistikkDto> = listOf(
            SykefraværsstatistikkDto(
                orgnr = "987654321",
                årstall = 2024,
                kvartal = 3,
                prosent = BigDecimal(10.00),
                tapteDagsverk = BigDecimal(12.00),
                muligeDagsverk = BigDecimal(120.00),
                tapteDagsverkGradert = BigDecimal(0),
                tapteDagsverkPerVarighet = listOf(
                    TapteDagsverkPerVarighetDto(
                        varighet = "A",
                        tapteDagsverk = BigDecimal(3.000002)
                    )
                ),
                antallPersoner = BigDecimal(4),
                sektor = "3",
                primærnæring = "68",
                primærnæringskode = "68209",
                rectype = "1",
            ),
            SykefraværsstatistikkDto(
                orgnr = "987654321",
                årstall = 2024,
                kvartal = 3,
                prosent = BigDecimal(11.00),
                tapteDagsverk = BigDecimal(11.00),
                muligeDagsverk = BigDecimal(100.00),
                tapteDagsverkGradert = BigDecimal(0),
                tapteDagsverkPerVarighet = listOf(
                    TapteDagsverkPerVarighetDto(
                        varighet = "A",
                        tapteDagsverk = BigDecimal(10.000002)
                    ),
                    TapteDagsverkPerVarighetDto(
                        varighet = "C",
                        tapteDagsverk = BigDecimal(1.00)
                    )
                ),
                antallPersoner = BigDecimal(4),
                sektor = "3",
                primærnæring = "68",
                primærnæringskode = "68209",
                rectype = "1",
            )
        )
        val prosent = beregnSykefraværsprosentForLand(statistikk)

        prosent shouldBe 10.5.toBigDecimal()
    }
}