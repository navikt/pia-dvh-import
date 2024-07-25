package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.domene.StatistikkImportService.Companion.beregnSykefraværsprosentForLand
import no.nav.pia.dvhimport.importjobb.domene.StatistikkImportService.Companion.tilGeneriskStatistikk
import no.nav.pia.dvhimport.importjobb.domene.StatistikkImportService.Companion.tilVirksomhetSykefraværsstatistikkDto
import java.math.BigDecimal
import kotlin.test.Test


class StatistikkImportServiceTest{
    @Test
    fun `dersom innhold er feil formattert, log objektet som er feil og ignore innhold`() {
        val json = """
            [{
              "årstall": 2024,
              "kvartal": 3,
              "orgnr": "321456789",
              "prosent": "68.9876",
              "finnesIkke": "Dette feltet gjør at deserialization vil feile"
            }, 
            {
              "årstall": 2024,
              "kvartal": 3,
              "orgnr": "321456789",
              "prosent": "68.9876",
              "tapteDagsverk": "120.23",
              "muligeDagsverk": "77.8716",
              "antallPersoner": "40.456",
              "rectype": "1"
            }]
        """.trimIndent()

        val statistikkDtoListe: List<VirksomhetSykefraværsstatistikkDto> = json.tilGeneriskStatistikk().tilVirksomhetSykefraværsstatistikkDto()
        statistikkDtoListe shouldHaveSize 1
        // TODO: hent log
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
        val prosent = beregnSykefraværsprosentForLand(statistikk)

        prosent shouldBe 10.5.toBigDecimal()
    }
}