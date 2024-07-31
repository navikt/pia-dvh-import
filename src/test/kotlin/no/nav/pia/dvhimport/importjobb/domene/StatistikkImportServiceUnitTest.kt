package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.domene.StatistikkImportService.Companion.kalkulerSykefraværsprosent
import java.math.BigDecimal
import kotlin.test.Test


class StatistikkImportServiceUnitTest{

    @Test
    fun `mapper JsonArray til array of strings`() {
        val result = """
                [
                  {"testField": "should fail"}
                ]
            """.trimIndent().tilGeneriskStatistikk()

        result.size shouldBe 1
    }

    @Test
    fun `kan regne ut sykefraværsprosent`() {
        val statistikk: List<VirksomhetSykefraværsstatistikkDto> = listOf(
            VirksomhetSykefraværsstatistikkDto(
                orgnr = "987654321",
                årstall = 2024,
                kvartal = 3,
                prosent = BigDecimal(10.00),
                tapteDagsverk = BigDecimal(12.00),
                muligeDagsverk = BigDecimal(120.00),
                antallPersoner = BigDecimal(4),
                rectype = "1",
            ),
            VirksomhetSykefraværsstatistikkDto(
                orgnr = "987654321",
                årstall = 2024,
                kvartal = 3,
                prosent = BigDecimal(11.00),
                tapteDagsverk = BigDecimal(11.00),
                muligeDagsverk = BigDecimal(100.00),
                antallPersoner = BigDecimal(4),
                rectype = "1",
            )
        )
        val prosent = kalkulerSykefraværsprosent(statistikk)

        prosent shouldBe 10.5.toBigDecimal()
    }
}