package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlin.test.Test


class SykefraværsstatistikkDtoTest {

    @Test
    fun `Parse en JSON String til SykefraværsstatistikkDto (med BigDecimal verdier som String)`() {
        val json = """
            {
              "årstall": 2024,
              "kvartal": 3,
              "orgnr": "321456789",
              "prosent": "68.9876",
              "tapteDagsverk": "120.23",
              "muligeDagsverk": "77.8716",
              "tapteDagsverkGradert": "1.80",
               "tapteDagsverkPerVarighet": [
                {
                  "varighet": "A",
                  "tapteDagsverk": "39.4"
                },
                {
                  "varighet": "C",
                  "tapteDagsverk": "1.5"
                }
              ],               
              "antallPersoner": 40,
              "sektor": "3",
              "primærnæring": "68",
              "primærnæringskode": "68209",
              "rectype": "1"
            }

        """.trimIndent()
        val dto = Json.decodeFromString<SykefraværsstatistikkDto>(json)

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 3
        dto.orgnr shouldBe "321456789"
        dto.prosent shouldBe 68.9876.toBigDecimal()
        dto.tapteDagsverk shouldBe 120.23.toBigDecimal()
        dto.muligeDagsverk shouldBe 77.8716.toBigDecimal()
        dto.tapteDagsverkGradert shouldBe 1.80.toBigDecimal().setScale(2)
        dto.antallPersoner shouldBe 40
    }

    @Test
    fun `Skal kunne parse BigDecimal verdier som Raw Content`() {
        val json = """
            {
              "årstall": 2024,
              "kvartal": 3,
              "orgnr": "321456789",
              "prosent": 68.9876,
              "tapteDagsverk": 120.23,
              "muligeDagsverk": 77.8716,
              "tapteDagsverkGradert": 1.80,
              "tapteDagsverkPerVarighet": [
                {
                  "varighet": "A",
                  "tapteDagsverk": 39.4
                },
                {
                  "varighet": "C",
                  "tapteDagsverk": 1.5
                }
              ], 
              "antallPersoner":40,
              "sektor": "3",
              "primærnæring": "68",
              "primærnæringskode": "68209",
              "rectype": "1"
            }
        """.trimIndent()
        val dto = Json.decodeFromString<SykefraværsstatistikkDto>(json)

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 3
        dto.orgnr shouldBe "321456789"
        dto.prosent shouldBe 68.9876.toBigDecimal()
        dto.tapteDagsverk shouldBe 120.23.toBigDecimal()
        dto.muligeDagsverk shouldBe 77.8716.toBigDecimal()
        dto.tapteDagsverkGradert shouldBe 1.80.toBigDecimal().setScale(2)
        dto.antallPersoner shouldBe 40
    }
}