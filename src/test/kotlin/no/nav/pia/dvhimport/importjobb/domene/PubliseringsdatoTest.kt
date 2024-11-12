package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import kotlinx.datetime.LocalDateTime
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.antallDagerTilPubliseringsdato
import kotlin.test.Test

class PubliseringsdatoTest {
    val iDag = LocalDateTime.parse("2024-05-30T08:00:00")
    val publiseringsdatoIFremtiden = Publiseringsdato(
        årstall = 2024,
        kvartal = 3,
        offentligDato = LocalDateTime.parse("2024-11-28T08:00:00"),
    )
    val publiseringsdatoErIDag = Publiseringsdato(
        årstall = 2024,
        kvartal = 1,
        offentligDato = iDag,
    )
    val publiseringsdatoErPassert = Publiseringsdato(
        årstall = 2023,
        kvartal = 4,
        offentligDato = LocalDateTime.parse("2024-02-29T08:00:00"),
    )

    @Test
    fun `kalkuler antall dager fra dato til neste publisertdato -- samme dag`() {
        iDag.antallDagerTilPubliseringsdato(publiseringsdatoErIDag) shouldBe 0
    }

    @Test
    fun `kalkuler antall dager fra dato til neste publisertdato -- dato i fremtiden`() {
        iDag.antallDagerTilPubliseringsdato(publiseringsdatoIFremtiden) shouldBe 182
    }

    @Test
    fun `kalkuler antall dager fra dato til neste publisertdato -- publiseringsdato er passert`() {
        iDag.antallDagerTilPubliseringsdato(publiseringsdatoErPassert) shouldBe -91
    }
}
