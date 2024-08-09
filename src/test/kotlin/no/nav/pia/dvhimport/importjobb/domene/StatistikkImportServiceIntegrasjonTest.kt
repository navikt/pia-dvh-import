package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.integrasjoner.jobbsender.Jobb
import io.kotest.inspectors.forAtLeastOne
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import no.nav.pia.dvhimport.importjobb.domene.StatistikkImportService.Companion.tilFilnavn
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import java.math.BigDecimal
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test


class StatistikkImportServiceIntegrasjonTest {
    private val gcsContainer = TestContainerHelper.googleCloudStorage
    private val kafkaContainer = TestContainerHelper.kafka
    private val eksportertStatistikkKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER)


    @BeforeTest
    fun setup() {
        gcsContainer.opprettTestBucketHvisIkkeFunnet()
        /*
         GCS Rest API er tilgjengelig fra eksponert port (dynamic port) på localhost (kjør test i debug)
         f.eks: http://localhost:{dynamic_port}/storage/v1/b/fake-gcs-bucket-in-container/o/land.json
        */
        eksportertStatistikkKonsument.subscribe(mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER.navnMedNamespace))
    }

    @AfterTest
    fun tearDown() {
        eksportertStatistikkKonsument.unsubscribe()
        eksportertStatistikkKonsument.close()
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

        kafkaContainer.sendJobbMelding(Jobb.landSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'LAND'".toRegex()
        dvhImportApplikasjon shouldContainLog "Fikk exception i import prosess med melding 'Encountered an unknown key 'testField'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'landSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `import statistikk LAND og send statistikk til Kafka`() {
        lagTestDataForLand()
        val nøkkel = """{"kvartal":"2024K1","meldingType":"SYKEFRAVÆRSSTATISTIKK-LAND"}"""

        kafkaContainer.sendJobbMelding(Jobb.landSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'LAND'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori LAND er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'landSykefraværsstatistikkDvhImport' ferdig".toRegex()

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                key = nøkkel,
                konsument = eksportertStatistikkKonsument
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<LandSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 1
                deserialiserteSvar.forAtLeastOne { landStatistikk ->
                    landStatistikk.land shouldBe "NO"
                    landStatistikk.årstall shouldBe 2024
                    landStatistikk.kvartal shouldBe 1
                }
            }
        }
    }

    @Test
    fun `import statistikk SEKTOR`() {
        lagTestDataForSektor()

        kafkaContainer.sendJobbMelding(Jobb.sektorSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'SEKTOR'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori SEKTOR er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'sektorSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `import statistikk NÆRING`() {
        lagTestDataForNæring()

        kafkaContainer.sendJobbMelding(Jobb.næringSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'NÆRING'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRING er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'næringSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `import statistikk NÆRINGSKODE`() {
        lagTestDataForNæringskode()

        kafkaContainer.sendJobbMelding(Jobb.næringskodeSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'NÆRINGSKODE'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRINGSKODE er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'næringskodeSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `import statistikk VIRKSOMHET`() {
        lagTestDataForVirksomhet()

        kafkaContainer.sendJobbMelding(Jobb.virksomhetSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'VIRKSOMHET'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori VIRKSOMHET er: '26.0'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'virksomhetSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `import statistikk VIRKSOMHET_METADATA`() {
        lagTestDataForVirksomhetMetadata()

        kafkaContainer.sendJobbMelding(Jobb.virksomhetMetadataSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av virksomhet metadata".toRegex()
        dvhImportApplikasjon shouldContainLog "Importert metadata for '1' virksomhet-er".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'virksomhetMetadataSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `import statistikk for alle kategorier`() {
        lagTestDataForLand()
        lagTestDataForSektor()
        lagTestDataForNæring()
        lagTestDataForNæringskode()
        lagTestDataForVirksomhet()
        lagTestDataForVirksomhetMetadata()

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for alle statistikkkategorier".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori LAND er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori SEKTOR er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRING er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRINGSKODE er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori VIRKSOMHET er: '26.0'".toRegex()
        dvhImportApplikasjon shouldContainLog "Importert metadata for '1' virksomhet-er".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'alleKategorierSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }


    private fun lagTestDataForLand(
        land: String = "NO",
        årstall: Int = 2024,
        kvartal: Int = 1,
        prosent: BigDecimal = BigDecimal(6.2),
        tapteDagsverk: BigDecimal = BigDecimal(8894426.768373),
        muligeDagsverk: BigDecimal = BigDecimal(143458496.063556),
        antallPersoner: BigDecimal = BigDecimal(3124427),
    ): LandSykefraværsstatistikkDto {
        val filnavn = Statistikkategori.LAND.tilFilnavn()
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
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

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
        verifiserBlobFinnes shouldBe true
        return LandSykefraværsstatistikkDto(
            land = land,
            årstall = årstall,
            kvartal = kvartal,
            prosent = prosent,
            tapteDagsverk = tapteDagsverk,
            muligeDagsverk = muligeDagsverk,
            antallPersoner = antallPersoner,
        )
    }

    private fun lagTestDataForSektor() {
        val filnavn = Statistikkategori.SEKTOR.tilFilnavn()
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
            bytes = """
            [{
              "årstall": 2024,
              "kvartal": 1,
              "sektor": "2",
              "prosent": "6.2",
              "tapteDagsverk": "88944.768373",
              "muligeDagsverk": "1434584.063556",
              "antallPersoner": "3124427"
            },
            {
             "årstall": 2024,
             "kvartal": 1,
             "sektor": "3",
             "prosent": "2.7",
             "tapteDagsverk": "94426.768373",
             "muligeDagsverk": "3458496.063556",
             "antallPersoner": "24427"
            }]
            """.trimIndent().encodeToByteArray()
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
        verifiserBlobFinnes shouldBe true
    }

    private fun lagTestDataForNæring() {
        val filnavn = Statistikkategori.NÆRING.tilFilnavn()
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
            bytes = """
            [{
              "årstall": 2024,
              "kvartal": 1,
              "næring": "02",
              "prosent": "6.2",
              "tapteDagsverk": "88944.768373",
              "muligeDagsverk": "1434584.063556",
              "antallPersoner": "3124427"
            },
            {
             "årstall": 2024,
             "kvartal": 1,
             "næring": "88",
             "prosent": "2.7",
             "tapteDagsverk": "94426.768373",
             "muligeDagsverk": "3458496.063556",
             "antallPersoner": "24427"
            }]
            """.trimIndent().encodeToByteArray()
        )
    }

    private fun lagTestDataForNæringskode() {
        val filnavn = Statistikkategori.NÆRINGSKODE.tilFilnavn()
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
            bytes = """
            [{
              "årstall": 2024,
              "kvartal": 1,
              "næringskode": "02300",
              "prosent": "6.2",
              "tapteDagsverk": "88944.768373",
              "muligeDagsverk": "1434584.063556",
              "antallPersoner": "3124427"
            },
            {
             "årstall": 2024,
             "kvartal": 1,
             "næringskode": "88911",
             "prosent": "2.7",
             "tapteDagsverk": "94426.768373",
             "muligeDagsverk": "3458496.063556",
             "antallPersoner": "24427"
            }]
            """.trimIndent().encodeToByteArray()
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
        verifiserBlobFinnes shouldBe true
    }

    private fun lagTestDataForVirksomhet() {
        val filnavn = Statistikkategori.VIRKSOMHET.tilFilnavn()
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
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

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
        verifiserBlobFinnes shouldBe true
    }

    private fun lagTestDataForVirksomhetMetadata() {
        val filnavn = Statistikkategori.VIRKSOMHET_METADATA.tilFilnavn()
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
            bytes = """
            [{
              "årstall": 2024,
              "kvartal": 3,
              "orgnr": "987654321",
              "sektor": "2",
              "primærnæring": "88",
              "primærnæringskode": "88.911",
              "rectype": "1"
            }]
            """.trimIndent().encodeToByteArray()
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
        verifiserBlobFinnes shouldBe true
    }
}