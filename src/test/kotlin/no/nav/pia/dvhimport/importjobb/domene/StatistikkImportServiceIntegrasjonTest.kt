package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.integrasjoner.jobbsender.Jobb.importSykefraværKvartalsstatistikk
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import kotlin.test.BeforeTest
import kotlin.test.Test


class StatistikkImportServiceIntegrasjonTest {
    private val gcsContainer = TestContainerHelper.googleCloudStorage
    private val kafkaContainer = TestContainerHelper.kafka

    @BeforeTest
    fun setup() {
        gcsContainer.opprettTestBucketHvisIkkeFunnet()
    }

    @Test
    fun `dersom innhold er feil formattert, log objektet som er feil (uten orgnr) og ignorer innhold`() {

        gcsContainer.lagreTestBlob(
            blobNavn = "land.json",
            bytes =
            """
                [
                  {
                    "land": "NO",
                    "testField": "should fail", 
                    "noeSomLignerEtOrgnr": "987654321"
                  }
                ]
            """.trimIndent().encodeToByteArray()
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = "land.json")
        verifiserBlobFinnes shouldBe true

        kafkaContainer.sendJobbMelding(importSykefraværKvartalsstatistikk)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for alle statistikkkategorier".toRegex()
        dvhImportApplikasjon shouldContainLog "Fikk exception i import prosess med melding 'Encountered an unknown key 'testField'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'importSykefraværKvartalsstatistikk' ferdig".toRegex()
    }

    @Test
    fun `import statistikk LAND`() {
        /*
                      "land": "NO",
              "årstall": 2024,
              "kvartal": 1,
              "prosent": 6.2,
              "tapteDagsverk": 8894426.768373,
              "muligeDagsverk": 143458496.063556,
              "antallPersoner": 3124427

         */

        gcsContainer.lagreTestBlob(
            blobNavn = "land.json",
            bytes = """
            [{
              "årstall": 2024,
              "kvartal": 1,
              "land": "NO",
              "prosent": "6.2",
              "tapteDagsverk": "8894426.768373",
              "muligeDagsverk": "143458496.063556",
              "antallPersoner": "3124427"
            }]
            """.trimIndent().encodeToByteArray()
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = "land.json")
        verifiserBlobFinnes shouldBe true

        kafkaContainer.sendJobbMelding(importSykefraværKvartalsstatistikk)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for alle statistikkkategorier".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori LAND er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'importSykefraværKvartalsstatistikk' ferdig".toRegex()
    }

    @Test
    fun `import statistikk VIRKSOMHET`() {

        gcsContainer.lagreTestBlob(
            blobNavn = "land.json",
            bytes = """
            [{
              "årstall": 2024,
              "kvartal": 3,
              "land": "NO",
              "prosent": "5.0",
              "tapteDagsverk": "20.00",
              "muligeDagsverk": "100.00",
              "antallPersoner": "10"
            }]
            """.trimIndent().encodeToByteArray()
        )
        gcsContainer.lagreTestBlob(
            blobNavn = "virksomhet.json",
            bytes = """
            [{
              "årstall": 2024,
              "kvartal": 3,
              "orgnr": "987654321",
              "prosent": "26.0",
              "tapteDagsverk": "20.23",
              "muligeDagsverk": "77.8716",
              "antallPersoner": "40.456", 
              "rectype": "1"
            }]
            """.trimIndent().encodeToByteArray()
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = "virksomhet.json")
        verifiserBlobFinnes shouldBe true

        kafkaContainer.sendJobbMelding(importSykefraværKvartalsstatistikk)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for alle statistikkkategorier".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori VIRKSOMHET er: '26.0'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'importSykefraværKvartalsstatistikk' ferdig".toRegex()
    }
}