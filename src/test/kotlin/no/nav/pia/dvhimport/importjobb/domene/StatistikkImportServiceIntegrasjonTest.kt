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
        lagTestDataForLand().lagreITestBucket(kategori = Statistikkategori.LAND, nøkkel = "land", verdi = "NO")

        kafkaContainer.sendJobbMelding(Jobb.landSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'LAND'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori LAND er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'landSykefraværsstatistikkDvhImport' ferdig".toRegex()

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                key = """{"kvartal":"2024K1","meldingType":"SYKEFRAVÆRSSTATISTIKK-LAND"}""",
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
                    landStatistikk.tapteDagsverk shouldBe 8894426.768373.toBigDecimal()
                    landStatistikk.muligeDagsverk shouldBe 143458496.063556.toBigDecimal()
                    landStatistikk.antallPersoner shouldBe 3124427.toBigDecimal()
                    landStatistikk.prosent shouldBe 6.2.toBigDecimal()

                }
            }
        }
    }

    @Test
    fun `import statistikk SEKTOR og send statistikk til Kafka`() {
        lagTestDataForSektor()

        kafkaContainer.sendJobbMelding(Jobb.sektorSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'SEKTOR'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori SEKTOR er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'sektorSykefraværsstatistikkDvhImport' ferdig".toRegex()

        val nøkkel = """{"kvartal":"2024K1","meldingType":"SYKEFRAVÆRSSTATISTIKK-SEKTOR"}"""
        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                key = nøkkel,
                konsument = eksportertStatistikkKonsument
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<SektorSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 2
                deserialiserteSvar.filter { it.sektor == "3" }.forAtLeastOne { sektorStatistikk ->
                    sektorStatistikk.sektor shouldBe "3"
                    sektorStatistikk.årstall shouldBe 2024
                    sektorStatistikk.kvartal shouldBe 1
                    sektorStatistikk.prosent shouldBe 2.7.toBigDecimal()
                    sektorStatistikk.tapteDagsverk shouldBe 94426.768373.toBigDecimal()
                    sektorStatistikk.muligeDagsverk shouldBe 3458496.063556.toBigDecimal()
                    sektorStatistikk.antallPersoner shouldBe 24427.toBigDecimal()
                }
                deserialiserteSvar.filter { it.sektor == "2" }.forAtLeastOne { sektorStatistikk ->
                    sektorStatistikk.sektor shouldBe "2"
                    sektorStatistikk.årstall shouldBe 2024
                    sektorStatistikk.kvartal shouldBe 1
                    sektorStatistikk.prosent shouldBe 6.2.toBigDecimal()
                    sektorStatistikk.tapteDagsverk shouldBe 88944.768373.toBigDecimal()
                    sektorStatistikk.muligeDagsverk shouldBe 1434584.063556.toBigDecimal()
                    sektorStatistikk.antallPersoner shouldBe 3124427.toBigDecimal()
                }
            }
        }
    }

    @Test
    fun `import statistikk NÆRING og send statistikk til Kafka`() {
        lagTestDataForNæring()

        kafkaContainer.sendJobbMelding(Jobb.næringSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'NÆRING'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRING er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'næringSykefraværsstatistikkDvhImport' ferdig".toRegex()

        val nøkkel = """{"kvartal":"2024K1","meldingType":"SYKEFRAVÆRSSTATISTIKK-NÆRING"}"""
        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                key = nøkkel,
                konsument = eksportertStatistikkKonsument
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<NæringSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 2
                deserialiserteSvar.filter { it.næring == "88" }.forAtLeastOne { næringStatistikk ->
                    næringStatistikk.næring shouldBe "88"
                    næringStatistikk.årstall shouldBe 2024
                    næringStatistikk.kvartal shouldBe 1
                    næringStatistikk.prosent shouldBe 2.7.toBigDecimal()
                    næringStatistikk.tapteDagsverk shouldBe 94426.768373.toBigDecimal()
                    næringStatistikk.muligeDagsverk shouldBe 3458496.063556.toBigDecimal()
                    næringStatistikk.antallPersoner shouldBe 24427.toBigDecimal()
                }
                deserialiserteSvar.filter { it.næring == "02" }.forAtLeastOne { næringStatistikk ->
                    næringStatistikk.næring shouldBe "02"
                    næringStatistikk.årstall shouldBe 2024
                    næringStatistikk.kvartal shouldBe 1
                    næringStatistikk.prosent shouldBe 6.2.toBigDecimal()
                    næringStatistikk.tapteDagsverk shouldBe 88944.768373.toBigDecimal()
                    næringStatistikk.muligeDagsverk shouldBe 1434584.063556.toBigDecimal()
                    næringStatistikk.antallPersoner shouldBe 3124427.toBigDecimal()
                }
            }
        }

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
        lagTestDataForLand().lagreITestBucket(kategori = Statistikkategori.LAND, nøkkel = "land", verdi = "NO")
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
        prosent: BigDecimal = 6.2.toBigDecimal(),
        tapteDagsverk: BigDecimal = 8894426.768373.toBigDecimal(),
        muligeDagsverk: BigDecimal = 143458496.063556.toBigDecimal(),
        antallPersoner: BigDecimal = 3124427.toBigDecimal(),
    ): LandSykefraværsstatistikkDto {
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

    private fun SykefraværsstatistikkDto.lagreITestBucket(
        kategori: Statistikkategori,
        nøkkel: String,
        verdi: String,
    ) {
        val filnavn = kategori.tilFilnavn()
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
            bytes = """
            [{
              "årstall": ${this.årstall},
              "kvartal": ${this.kvartal},
              "$nøkkel": "$verdi",
              "prosent": "${this.prosent}",
              "tapteDagsverk": "${this.tapteDagsverk}",
              "muligeDagsverk": "${this.muligeDagsverk}",
              "antallPersoner": "${this.antallPersoner}"
            }]
            """.trimIndent().encodeToByteArray()
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
        verifiserBlobFinnes shouldBe true
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
              "tapteDagsverkGradert": 90.034285,
              "tapteDagsverkPerVarighet": [
                {
                  "varighet": "D",
                  "tapteDagsverk": 148.534285
                }
              ],
              "antallPersoner": "3124427"
            },
            {
             "årstall": 2024,
             "kvartal": 1,
             "næring": "88",
             "prosent": "2.7",
             "tapteDagsverk": "94426.768373",
             "muligeDagsverk": "3458496.063556",
             "tapteDagsverkGradert": 90.034285,
             "tapteDagsverkPerVarighet": [
               {
                 "varighet": "D",
                 "tapteDagsverk": 148.534285
               }
             ],
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
              "tapteDagsverkGradert": 90.034285,
              "tapteDagsverkPerVarighet": [
                {
                  "varighet": "D",
                  "tapteDagsverk": 148.534285
                }
              ],
              "antallPersoner": "3124427"
            },
            {
             "årstall": 2024,
             "kvartal": 1,
             "næringskode": "88911",
             "prosent": "2.7",
             "tapteDagsverk": "94426.768373",
             "muligeDagsverk": "3458496.063556",
             "tapteDagsverkGradert": 90.034285,
             "tapteDagsverkPerVarighet": [
               {
                 "varighet": "D",
                 "tapteDagsverk": 148.534285
               }
             ],
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