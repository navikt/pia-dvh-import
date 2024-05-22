package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlin.test.Test


class SykefraværsstatistikkDtoTest {

    @Test
    fun `Parse en JSON String til SykefraværsstatistikkDto (med BigDecimal verdier som String)`() {
        val json = """
            {
              "ARSTALL":2024,
              "KVARTAL":3,
              "ORGNR":"321456789",
              "NARING":"68",
              "NARING_KODE":"68209",
              "PRIMARNARINGSKODE":"68.209",
              "SEKTOR":"3",
              "VARIGHET":"X",
              "RECTYPE":"1",
              "TAPTEDV":"120.23",
              "MULIGEDV":"77.8716",
              "ANTALL_GS":"5.5",
              "TAPTEDV_GS":"1.80",
              "ANTPERS":40
            }
        """.trimIndent()
        val dto = Json.decodeFromString<SykefraværsstatistikkDto>(json)

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 3
        dto.orgnr shouldBe "321456789"
        dto.tapteDagsverk shouldBe 120.23.toBigDecimal()
        dto.muligeDagsverk shouldBe 77.8716.toBigDecimal()
        dto.tapteDagsverkGs shouldBe 1.80.toBigDecimal().setScale(2)
        dto.antallDagsverkGs shouldBe 5.5.toBigDecimal()
        dto.antallPersoner shouldBe 40
    }

    @Test
    fun `Skal kunne parse BigDecimal verdier som Raw Content`() {
        val json = """
            {
              "ARSTALL": 2024,
              "KVARTAL": 3,
              "ORGNR": "321456789",
              "NARING": "68",
              "NARING_KODE": "68209",
              "PRIMARNARINGSKODE": "68.209",
              "SEKTOR": "3",
              "VARIGHET": "X",
              "RECTYPE": "1",
              "TAPTEDV": 120.23,
              "MULIGEDV": 77.8716,
              "ANTALL_GS": 5.5,
              "TAPTEDV_GS": 1.80,
              "ANTPERS":40
            }
        """.trimIndent()
        val dto = Json.decodeFromString<SykefraværsstatistikkDto>(json)

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 3
        dto.orgnr shouldBe "321456789"
        dto.tapteDagsverk shouldBe 120.23.toBigDecimal()
        dto.muligeDagsverk shouldBe 77.8716.toBigDecimal()
        dto.tapteDagsverkGs shouldBe 1.80.toBigDecimal().setScale(2)
        dto.antallDagsverkGs shouldBe 5.5.toBigDecimal()
        dto.antallPersoner shouldBe 40
    }
}