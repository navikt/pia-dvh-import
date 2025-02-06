package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import kotlinx.datetime.LocalDateTime
import kotlin.test.Test

class MottattPubliseringsdatoFraDvhDtoUnitTest {
    @Test
    fun `Publiseringsdato har riktig datoformat`() {
        val json =
            """
            {
              "rapport_periode": "202403",
              "offentlig_dato": "2024-11-28, 08:00:00",  
              "oppdatert_i_dvh": "2023-10-20, 08:00:00"
             }
            """.trimIndent()

        val publiseringsdatoFraDvhDto = json.tilPubliseringsdatoFraDvhDto()
        publiseringsdatoFraDvhDto.rapportPeriode shouldBe "202403"
        publiseringsdatoFraDvhDto.offentligDato shouldBe LocalDateTime.parse("2024-11-28T08:00:00")
        publiseringsdatoFraDvhDto.offentligDato shouldBe LocalDateTime.parse("2024-11-28T08:00") // legit ISO-8601 format
    }
}
