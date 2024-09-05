package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import kotlinx.datetime.LocalDateTime
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.antallDagerTilPubliseringsdato
import kotlin.test.Test


class PubliseringsdatoTest {
    val IDAG = LocalDateTime.parse("2024-05-30T08:00:00")
    val PUBLISERINGSDATO_I_FREMTIDEN = Publiseringsdato(
        årstall = 2024,
        kvartal = 3,
        offentligDato = LocalDateTime.parse("2024-11-28T08:00:00")
    )
    val PUBLISERINGSDATO_ER_IDAG = Publiseringsdato(
        årstall = 2024,
        kvartal = 1,
        offentligDato = IDAG
    )
    val PUBLISERINGSDATO_ER_PASSERT = Publiseringsdato(
        årstall = 2023,
        kvartal = 4,
        offentligDato = LocalDateTime.parse("2024-02-29T08:00:00")
    )

    @Test
    fun `kalkuler antall dager fra dato til neste publisertdato -- samme dag`() {
        IDAG.antallDagerTilPubliseringsdato(PUBLISERINGSDATO_ER_IDAG) shouldBe 0
    }

    @Test
    fun `kalkuler antall dager fra dato til neste publisertdato -- dato i fremtiden`() {
        IDAG.antallDagerTilPubliseringsdato(PUBLISERINGSDATO_I_FREMTIDEN) shouldBe 182
    }

    @Test
    fun `kalkuler antall dager fra dato til neste publisertdato -- publiseringsdato er passert`() {
        IDAG.antallDagerTilPubliseringsdato(PUBLISERINGSDATO_ER_PASSERT) shouldBe -91
    }
}