package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import kotlinx.datetime.LocalDateTime
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.kalkulerSykefraværsprosent
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.nestePubliseringsdato
import java.math.BigDecimal
import kotlin.test.Test

class ImportServiceUnitTest {
    @Test
    fun `mapper JsonArray til array of strings`() {
        val result =
            """
            [
              {"testField": "should fail"}
            ]
            """.trimIndent().tilListe()

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
                tapteDagsverkGradert = BigDecimal(0.00),
                tapteDagsverkPerVarighet = emptyList(),
                antallPersoner = 4,
                rectype = "1",
            ),
            VirksomhetSykefraværsstatistikkDto(
                orgnr = "987654321",
                årstall = 2024,
                kvartal = 3,
                prosent = BigDecimal(11.00),
                tapteDagsverk = BigDecimal(11.00),
                muligeDagsverk = BigDecimal(100.00),
                tapteDagsverkGradert = BigDecimal(0.00),
                tapteDagsverkPerVarighet = emptyList(),
                antallPersoner = 4,
                rectype = "1",
            ),
        )
        val prosent = kalkulerSykefraværsprosent(statistikk)

        prosent shouldBe 10.5.toBigDecimal()
    }

    @Test
    fun `finner ut neste publiseringsdato`() {
        val iDag = LocalDateTime.parse("2024-02-28T08:05:00")
        nestePubliseringsdato(publiseringsdatoDtoList(), iDag) shouldBe NestePubliseringsdato(
            årstall = 2023,
            kvartal = 4,
            dato = LocalDateTime.parse("2024-02-29T08:00:00"),
        )
    }

    @Test
    fun `finner ut neste publiseringsdato  -- samme dag som publiseringsdato`() {
        val iDag = LocalDateTime.parse("2024-09-05T15:00:00")
        nestePubliseringsdato(publiseringsdatoDtoList(), iDag) shouldBe NestePubliseringsdato(
            årstall = 2024,
            kvartal = 3,
            dato = LocalDateTime.parse("2024-11-28T08:00:00"),
        )
    }

    @Test
    fun `finner ut neste publiseringsdato  -- dagen før publiseringsdato`() {
        val iDag = LocalDateTime.parse("2024-09-04T08:05:00")
        nestePubliseringsdato(publiseringsdatoDtoList(), iDag) shouldBe NestePubliseringsdato(
            årstall = 2024,
            kvartal = 2,
            dato = LocalDateTime.parse("2024-09-05T08:00:00"),
        )
    }

    @Test
    fun `finner ut neste publiseringsdato  -- ingen neste publiseringsdato`() {
        val iDag = LocalDateTime.parse("2024-11-29T08:05:00")
        nestePubliseringsdato(publiseringsdatoDtoList(), iDag) shouldBe null
    }

    private fun publiseringsdatoDtoList(): List<PubliseringsdatoDto> {
        val publiseringsdatoer = listOf(
            PubliseringsdatoDto(
                rapportPeriode = "202304",
                offentligDato = LocalDateTime.parse("2024-02-29T08:00:00"),
                oppdatertIDvh = LocalDateTime.parse("2023-10-20T11:57:40"),
            ),
            PubliseringsdatoDto(
                rapportPeriode = "202401",
                offentligDato = LocalDateTime.parse("2024-05-30T08:00:00"),
                oppdatertIDvh = LocalDateTime.parse("2023-10-20T11:57:40"),
            ),
            PubliseringsdatoDto(
                rapportPeriode = "202402",
                offentligDato = LocalDateTime.parse("2024-09-05T08:00:00"),
                oppdatertIDvh = LocalDateTime.parse("2023-10-20T11:57:40"),
            ),
            PubliseringsdatoDto(
                rapportPeriode = "202403",
                offentligDato = LocalDateTime.parse("2024-11-28T08:00:00"),
                oppdatertIDvh = LocalDateTime.parse("2023-10-20T11:57:40"),
            ),
        )
        return publiseringsdatoer
    }
}
