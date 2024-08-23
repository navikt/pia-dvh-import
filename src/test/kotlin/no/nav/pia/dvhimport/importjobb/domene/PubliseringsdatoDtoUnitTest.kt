package no.nav.pia.dvhimport.importjobb.domene

import kotlin.test.Test


class PubliseringsdatoDtoUnitTest {

    @Test
    fun `Publiseringsdato har riktig datoformat`() {
        val json = """
              {
                "rapport_periode": "202403",
                "offentlig_dato": "2024-11-28, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20"
               }
        """.trimIndent()

        json.tilPubliseringsdatoDto()
    }
}