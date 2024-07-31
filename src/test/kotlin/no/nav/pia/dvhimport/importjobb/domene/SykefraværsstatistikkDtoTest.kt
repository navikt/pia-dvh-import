package no.nav.pia.dvhimport.importjobb.domene

import io.kotest.matchers.shouldBe
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
        dto.antallPersoner shouldBe 3124427.toBigDecimal()
    }


    @Test
    fun `Skal kunne parse en JSON String til VirksomhetSykefraværsstatistikkDto`() {
        val json = """
            [{
              "orgnr": "222222222",
              "årstall": "2024",
              "kvartal": "1",
              "prosent": 10.3,
              "tapteDagsverk": 73.28912,
              "muligeDagsverk": 708.9544,
              "rectype": "2",
              "antallPersoner": 10.00862,
              "varighet_a": null,
              "varighet_b": 3.2805,
              "varighet_c": null,
              "varighet_d": 10.00862,
              "varighet_e": 22,
              "varighet_f": 38
            }]
        """.trimIndent()
        val dto = json.tilGeneriskStatistikk().toSykefraværsstatistikkDto<VirksomhetSykefraværsstatistikkDto>().first()

        dto.årstall shouldBe 2024
        dto.kvartal shouldBe 1
        dto.orgnr shouldBe "222222222"
        dto.prosent shouldBe 10.3.toBigDecimal()
        dto.tapteDagsverk shouldBe 73.28912.toBigDecimal()
        dto.muligeDagsverk shouldBe 708.9544.toBigDecimal()
        dto.antallPersoner shouldBe 10.00862.toBigDecimal()
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
}