package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.assertions.json.shouldBeValidJson
import io.kotest.assertions.json.shouldContainJsonKey
import io.kotest.assertions.json.shouldContainJsonKeyValue
import io.kotest.assertions.json.shouldNotContainJsonKeyValue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldNotContain
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test


class SykefraværsstatistikkDtoTest {


    @Test
    fun `Skal kunne parse en JSON String til LandSykefraværsstatistikkDto`() {
        val json = """
            [{
              "land": "NO",
              "årstall": 2024,
              "kvartal": 1,
              "prosent": 6.2,
              "tapteDagsverk": 8894426.768373,
              "muligeDagsverk": 143458496.063556,
              "antallPersoner": 3124427
            }]
        """.trimIndent()
        val dto = json.tilGeneriskStatistikk().toSykefraværsstatistikkDto<LandSykefraværsstatistikkDto>().first()

        dto.land shouldBe "NO"
        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 1
        dto.prosent shouldBe 6.2.toBigDecimal()
        dto.tapteDagsverk shouldBe 8894426.768373.toBigDecimal()
        dto.muligeDagsverk shouldBe 143458496.063556.toBigDecimal()
        dto.antallPersoner shouldBe 3124427.toBigDecimal()
    }

    @Test
    fun `Skal kunne parse en JSON String til NæringSykefraværsstatistikkDto`() {
        val json = """
            [{
              "næring": "22",
              "årstall": 2024,
              "kvartal": 1,
              "prosent": 6.2,
              "tapteDagsverk": 8894426.768373,
              "muligeDagsverk": 143458496.063556,
              "tapteDagsverkGradert": 90.034285,
              "tapteDagsverkPerVarighet": [
                {
                  "varighet": "A",
                  "tapteDagsverk": 12.1527
                },
                {
                  "varighet": "B",
                  "tapteDagsverk": 2.7
                },
                {
                  "varighet": "C",
                  "tapteDagsverk": 15
                },
                {
                  "varighet": "D",
                  "tapteDagsverk": 148.534285
                },
                {
                  "varighet": "E",
                  "tapteDagsverk": 142.6
                },
                {
                  "varighet": "F",
                  "tapteDagsverk": 31.4
                }
              ],
              "antallPersoner": 3124427
            }]
        """.trimIndent()
        val dto = json.tilGeneriskStatistikk().toSykefraværsstatistikkDto<NæringSykefraværsstatistikkDto>().first()

        dto.næring shouldBe "22"
        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 1
        dto.prosent shouldBe 6.2.toBigDecimal()
        dto.tapteDagsverk shouldBe 8894426.768373.toBigDecimal()
        dto.muligeDagsverk shouldBe 143458496.063556.toBigDecimal()
        dto.tapteDagsverkGradert shouldBe 90.034285.toBigDecimal()
        dto.tapteDagsverkPerVarighet.size shouldBe 6
        dto.tapteDagsverkPerVarighet[0].varighet shouldBe "A"
        dto.tapteDagsverkPerVarighet[0].tapteDagsverk shouldBe 12.1527.toBigDecimal()
        dto.antallPersoner shouldBe 3124427.toBigDecimal()
    }


    @Test
    fun `Skal kunne parse en JSON String til VirksomhetSykefraværsstatistikkDto`() {
        val json = """
            [{
                "orgnr": "999888777",
                "årstall": "2024",
                "kvartal": "1",
                "prosent": 19.2,
                "tapteDagsverk": 352.386985,
                "muligeDagsverk": 1832.599301,
                "antallPersoner": 40,
                "rectype": "2",
                "tapteDagsverkGradert": 90.034285,
                "tapteDagsverkPerVarighet": [
                  {
                    "varighet": "A",
                    "tapteDagsverk": 12.1527
                  },
                  {
                    "varighet": "B",
                    "tapteDagsverk": 2.7
                  },
                  {
                    "varighet": "C",
                    "tapteDagsverk": 15
                  },
                  {
                    "varighet": "D",
                    "tapteDagsverk": 148.534285
                  },
                  {
                    "varighet": "E",
                    "tapteDagsverk": 142.6
                  },
                  {
                    "varighet": "F",
                    "tapteDagsverk": 31.4
                  }
                ]
              }]
        """.trimIndent()
        val dto = json.tilGeneriskStatistikk().toSykefraværsstatistikkDto<VirksomhetSykefraværsstatistikkDto>().first()

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 1
        dto.orgnr shouldBe "999888777"
        dto.prosent shouldBe 19.2.toBigDecimal()
        dto.tapteDagsverk shouldBe 352.386985.toBigDecimal()
        dto.muligeDagsverk shouldBe 1832.599301.toBigDecimal()
        dto.tapteDagsverkGradert shouldBe 90.034285.toBigDecimal()
        dto.antallPersoner shouldBe 40.toBigDecimal()
        dto.tapteDagsverkPerVarighet.size shouldBe 6
        dto.tapteDagsverkPerVarighet[0].varighet shouldBe "A"
        dto.tapteDagsverkPerVarighet[0].tapteDagsverk shouldBe 12.1527.toBigDecimal()
    }

    @Test
    fun `Skal kunne parse en JSON String til VirksomhetSykefraværsstatistikkDto (med BigDecimal verdier som String)`() {
        val json = """
            [{
              "årstall": 2024,
              "kvartal": 3,
              "orgnr": "321456789",
              "prosent": "68.9876",
              "tapteDagsverk": "120.23",
              "muligeDagsverk": "77.8716",
              "tapteDagsverkGradert": "90.034285",
              "tapteDagsverkPerVarighet": [
                {
                  "varighet": "A",
                  "tapteDagsverk": "12.1527"
                },
                {
                  "varighet": "B",
                  "tapteDagsverk": 2.7
                },
                {
                  "varighet": "C",
                  "tapteDagsverk": 15
                },
                {
                  "varighet": "D",
                  "tapteDagsverk": 148.534285
                },
                {
                  "varighet": "E",
                  "tapteDagsverk": 142.6
                },
                {
                  "varighet": "F",
                  "tapteDagsverk": 31.4
                }
              ],
              "antallPersoner": "40.456",
              "rectype": "1"
            }]
        """.trimIndent()
        val dto = json.tilGeneriskStatistikk().toSykefraværsstatistikkDto<VirksomhetSykefraværsstatistikkDto>().first()

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 3
        dto.orgnr shouldBe "321456789"
        dto.prosent shouldBe 68.9876.toBigDecimal()
        dto.tapteDagsverk shouldBe 120.23.toBigDecimal()
        dto.muligeDagsverk shouldBe 77.8716.toBigDecimal()
        dto.antallPersoner shouldBe 40.456.toBigDecimal()
    }

    @Test
    fun `Skal kunne parse en JSON String til VirksomhetSykefraværsstatistikkDto med null-verdier for varighet`() {
        val json = """
            [{
              "årstall": 2024,
              "kvartal": 3,
              "orgnr": "321456789",
              "prosent": "68.9876",
              "tapteDagsverk": "120.23",
              "muligeDagsverk": "77.8716",
              "tapteDagsverkGradert": "90.034285",
              "tapteDagsverkPerVarighet": [
                {
                  "varighet": "B",
                  "tapteDagsverk": null
                },
                {
                  "varighet": "C",
                  "tapteDagsverk": 15
                },
                {
                  "varighet": "D",
                  "tapteDagsverk": 148.534285
                },
                {
                  "varighet": "E",
                  "tapteDagsverk": 0
                },
                {
                  "varighet": "F",
                  "tapteDagsverk": null
                }
              ],
              "antallPersoner": "40.456",
              "rectype": "1"
            }]
        """.trimIndent()
        val dto = json.tilGeneriskStatistikk().toSykefraværsstatistikkDto<VirksomhetSykefraværsstatistikkDto>().first()

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 3
        dto.orgnr shouldBe "321456789"
        dto.prosent shouldBe 68.9876.toBigDecimal()
        dto.tapteDagsverk shouldBe 120.23.toBigDecimal()
        dto.muligeDagsverk shouldBe 77.8716.toBigDecimal()

        dto.antallPersoner shouldBe 40.456.toBigDecimal()
    }

    @Test
    fun `Skal ignorere varighet med null-verdier ved serialisering`() {
        val json = """
            [{
                "orgnr": "999888777",
                "årstall": "2024",
                "kvartal": "1",
                "prosent": 19.2,
                "tapteDagsverk": 352.386985,
                "muligeDagsverk": 1832.599301,
                "antallPersoner": 40,
                "rectype": "2",
                "tapteDagsverkGradert": 90.034285,
                "tapteDagsverkPerVarighet": [
                  {
                    "varighet": "B",
                    "tapteDagsverk": null
                  },
                  {
                    "varighet": "C",
                    "tapteDagsverk": null
                  },
                  {
                    "varighet": "D",
                    "tapteDagsverk": 148.534285
                  },
                  {
                    "varighet": "F",
                    "tapteDagsverk": 0
                  }
                ]
              }]
        """.trimIndent()
        val dto = json.tilGeneriskStatistikk().toSykefraværsstatistikkDto<VirksomhetSykefraværsstatistikkDto>().first()

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 1
        dto.orgnr shouldBe "999888777"
        dto.prosent shouldBe 19.2.toBigDecimal()
        dto.tapteDagsverk shouldBe 352.386985.toBigDecimal()
        dto.muligeDagsverk shouldBe 1832.599301.toBigDecimal()
        dto.tapteDagsverkGradert shouldBe 90.034285.toBigDecimal()
        dto.antallPersoner shouldBe 40.toBigDecimal()
        dto.tapteDagsverkPerVarighet.size shouldBe 4
        dto.tapteDagsverkPerVarighet[0].varighet shouldBe "B"
        dto.tapteDagsverkPerVarighet[0].tapteDagsverk shouldBe null

        val serialisertDto = Json.encodeToString(dto)

        serialisertDto.shouldBeValidJson()
        serialisertDto shouldNotContain "null"
        serialisertDto shouldContainJsonKey "tapteDagsverkPerVarighet"
        serialisertDto shouldContainJsonKey "$.tapteDagsverkPerVarighet[0].varighet"
        serialisertDto.shouldNotContainJsonKeyValue("$.tapteDagsverkPerVarighet[0].varighet", "B")
        serialisertDto.shouldContainJsonKeyValue("$.tapteDagsverkPerVarighet[0].varighet", "D")
        serialisertDto.shouldContainJsonKeyValue("$.tapteDagsverkPerVarighet[0].tapteDagsverk", 148.534285.toBigDecimal())
    }
}