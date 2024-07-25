package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import kotlin.test.Test


class VirksomhetSykefraværsstatistikkDtoTest {

    /*
    {"land":"NO","årstall":"2024","kvartal":"1","prosent":6.2,"tapteDagsverk":8894426.768373,"muligeDagsverk":143458496.063556,"antallPersoner":3124427}
    */

    @Test
    fun `Skal kunne parse en JSON String til LandSykefraværsstatistikkDto`() {
        val json = """
            {
              "land": "NO",
              "årstall": 2024,
              "kvartal": 1,
              "prosent": 6.2,
              "tapteDagsverk": 8894426.768373,
              "muligeDagsverk": 143458496.063556,
              "antallPersoner": 3124427
            }
        """.trimIndent()
        val dto = Json.decodeFromString<LandSykefraværsstatistikkDto>(json)

        dto.land shouldBe "NO"
        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 1
        dto.prosent shouldBe 6.2.toBigDecimal()
        dto.tapteDagsverk shouldBe 8894426.768373.toBigDecimal()
        dto.muligeDagsverk shouldBe 143458496.063556.toBigDecimal()
        dto.antallPersoner shouldBe 3124427.toBigDecimal()
    }

    @Test
    fun `Skal kunne parse en JSON String til VirksomhetSykefraværsstatistikkDto`() {
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
              "antallPersoner":40.456,
              "sektor": "3",
              "primærnæring": "68",
              "primærnæringskode": "68209",
              "rectype": "1"
            }
        """.trimIndent()
        val dto = Json.decodeFromString<VirksomhetSykefraværsstatistikkDto>(json)

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 3
        dto.orgnr shouldBe "321456789"
        dto.prosent shouldBe 68.9876.toBigDecimal()
        dto.tapteDagsverk shouldBe 120.23.toBigDecimal()
        dto.muligeDagsverk shouldBe 77.8716.toBigDecimal()
        dto.tapteDagsverkGradert shouldBe 1.80.toBigDecimal().setScale(2)
        dto.antallPersoner shouldBe 40.456.toBigDecimal()
    }

    @Test
    fun `Skal kunne parse en JSON String til VirksomhetSykefraværsstatistikkDto (med BigDecimal verdier som String)`() {
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
              "antallPersoner": "40.456",
              "sektor": "3",
              "primærnæring": "68",
              "primærnæringskode": "68209",
              "rectype": "1"
            }
        """.trimIndent()
        val dto = Json.decodeFromString<VirksomhetSykefraværsstatistikkDto>(json)

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 3
        dto.orgnr shouldBe "321456789"
        dto.prosent shouldBe 68.9876.toBigDecimal()
        dto.tapteDagsverk shouldBe 120.23.toBigDecimal()
        dto.muligeDagsverk shouldBe 77.8716.toBigDecimal()
        dto.tapteDagsverkGradert shouldBe 1.80.toBigDecimal().setScale(2)
        dto.antallPersoner shouldBe 40.456.toBigDecimal()
    }


    private infix fun VirksomhetSykefraværsstatistikkDto.shouldBe(expected: VirksomhetSykefraværsstatistikkDto) {
        this.orgnr shouldBe expected.orgnr

    }
}