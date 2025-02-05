package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.definisjoner.bransjer.Bransje
import io.kotest.matchers.shouldBe
import kotlinx.datetime.LocalDateTime
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.RUTEBILTRANSPORT_NÆRINGSKODE
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.TURBILTRANSPORT_NÆRINGSKODE
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.aggreger
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.kalkulerOgLoggSykefraværsprosent
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.leggTil
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.nestePubliseringsdato
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.utleddBransjeStatistikk
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
    fun `kan summere ett TapteDagsverkPerVarighetDto i en liste av TapteDagsverkPerVarighetDto`() {
        val tapteDagsverkPerVarighetListe = mutableListOf(
            TapteDagsverkPerVarighetDto(
                varighet = "A",
                tapteDagsverk = 10.2.toBigDecimal(),
            ),
            TapteDagsverkPerVarighetDto(
                varighet = "D",
                tapteDagsverk = 5.2.toBigDecimal(),
            ),
        )

        val result = tapteDagsverkPerVarighetListe.leggTil(
            TapteDagsverkPerVarighetDto(
                varighet = "A",
                tapteDagsverk = 5.3.toBigDecimal(),
            ),
        )
        result shouldBe listOf(
            TapteDagsverkPerVarighetDto(
                varighet = "A",
                tapteDagsverk = 15.5.toBigDecimal(),
            ),
            TapteDagsverkPerVarighetDto(
                varighet = "D",
                tapteDagsverk = 5.2.toBigDecimal(),
            ),
        )
    }

    @Test
    fun `kan legge et TapteDagsverkPerVarighetDto til en liste av TapteDagsverkPerVarighetDto`() {
        val tapteDagsverkPerVarighet = mutableListOf(
            TapteDagsverkPerVarighetDto(
                varighet = "A",
                tapteDagsverk = 10.2.toBigDecimal(),
            ),
            TapteDagsverkPerVarighetDto(
                varighet = "D",
                tapteDagsverk = 5.2.toBigDecimal(),
            ),
        )

        val result = tapteDagsverkPerVarighet.leggTil(
            TapteDagsverkPerVarighetDto(
                varighet = "C",
                tapteDagsverk = 5.3.toBigDecimal(),
            ),
        )

        result shouldBe listOf(
            TapteDagsverkPerVarighetDto(
                varighet = "A",
                tapteDagsverk = 10.2.toBigDecimal(),
            ),
            TapteDagsverkPerVarighetDto(
                varighet = "C",
                tapteDagsverk = 5.3.toBigDecimal(),
            ),
            TapteDagsverkPerVarighetDto(
                varighet = "D",
                tapteDagsverk = 5.2.toBigDecimal(),
            ),
        )
    }

    @Test
    fun `kan aggregere to lister av TapteDagsverkPerVarighetDto`() {
        val tapteDagsverkPerVarighet = mutableListOf(
            TapteDagsverkPerVarighetDto(
                varighet = "A",
                tapteDagsverk = 110.5.toBigDecimal(),
            ),
            TapteDagsverkPerVarighetDto(
                varighet = "D",
                tapteDagsverk = 5.2.toBigDecimal(),
            ),
        )

        val result = tapteDagsverkPerVarighet.aggreger(
            listOf(
                TapteDagsverkPerVarighetDto(
                    varighet = "A",
                    tapteDagsverk = 10.2.toBigDecimal(),
                ),
                TapteDagsverkPerVarighetDto(
                    varighet = "B",
                    tapteDagsverk = 3.2.toBigDecimal(),
                ),
            ),
        )
        result shouldBe listOf(
            TapteDagsverkPerVarighetDto(
                varighet = "A",
                tapteDagsverk = 120.7.toBigDecimal(),
            ),
            TapteDagsverkPerVarighetDto(
                varighet = "B",
                tapteDagsverk = 3.2.toBigDecimal(),
            ),
            TapteDagsverkPerVarighetDto(
                varighet = "D",
                tapteDagsverk = 5.2.toBigDecimal(),
            ),
        )
    }

    @Test
    fun `kan regne ut bransje statistikk ut av en liste av statistikk for næringskoder`() {
        val næringskodeStatistikk: List<NæringskodeSykefraværsstatistikkDto> = listOf(
            NæringskodeSykefraværsstatistikkDto(
                næringskode = RUTEBILTRANSPORT_NÆRINGSKODE,
                årstall = 2024,
                kvartal = 4,
                prosent = 10.0.toBigDecimal(),
                tapteDagsverk = 300.00.toBigDecimal(),
                muligeDagsverk = 3000.00.toBigDecimal(),
                tapteDagsverkGradert = 150.00.toBigDecimal(),
                tapteDagsverkPerVarighet = listOf(
                    TapteDagsverkPerVarighetDto(
                        varighet = "A",
                        tapteDagsverk = 12.3.toBigDecimal(),
                    ),
                    TapteDagsverkPerVarighetDto(
                        varighet = "D",
                        tapteDagsverk = 5.2.toBigDecimal(),
                    ),
                ),
                antallPersoner = 2000,
            ),
            NæringskodeSykefraværsstatistikkDto(
                næringskode = TURBILTRANSPORT_NÆRINGSKODE,
                årstall = 2024,
                kvartal = 4,
                prosent = 5.0.toBigDecimal(),
                tapteDagsverk = 100.00.toBigDecimal(),
                muligeDagsverk = 2000.00.toBigDecimal(),
                tapteDagsverkGradert = 120.00.toBigDecimal(),
                tapteDagsverkPerVarighet = listOf(
                    TapteDagsverkPerVarighetDto(
                        varighet = "A",
                        tapteDagsverk = 10.0.toBigDecimal(),
                    ),
                    TapteDagsverkPerVarighetDto(
                        varighet = "B",
                        tapteDagsverk = 2.5.toBigDecimal(),
                    ),
                    TapteDagsverkPerVarighetDto(
                        varighet = "D",
                        tapteDagsverk = 5.2.toBigDecimal(),
                    ),
                ),
                antallPersoner = 4500,
            ),
        )

        næringskodeStatistikk.utleddBransjeStatistikk(
            bransje = Bransje.TRANSPORT,
            årstall = 2024,
            kvartal = 4,
        ) shouldBe BransjeSykefraværsstatistikkDto(
            bransje = Bransje.TRANSPORT.navn,
            årstall = 2024,
            kvartal = 4,
            prosent = 8.0.toBigDecimal(),
            tapteDagsverk = 400.00.toBigDecimal(),
            muligeDagsverk = 5000.00.toBigDecimal(),
            tapteDagsverkGradert = 270.00.toBigDecimal(),
            tapteDagsverkPerVarighet = listOf(
                TapteDagsverkPerVarighetDto(
                    varighet = "A",
                    tapteDagsverk = 22.3.toBigDecimal(),
                ),
                TapteDagsverkPerVarighetDto(
                    varighet = "B",
                    tapteDagsverk = 2.5.toBigDecimal(),
                ),
                TapteDagsverkPerVarighetDto(
                    varighet = "D",
                    tapteDagsverk = 10.4.toBigDecimal(),
                ),
            ),
            antallPersoner = 6500,
        )
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
        val prosent = kalkulerOgLoggSykefraværsprosent(StatistikkKategori.VIRKSOMHET, statistikk)

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
