package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.integrasjoner.jobbsender.Jobb
import io.kotest.inspectors.forAtLeastOne
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForLand
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæring
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæringskode
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForSektor
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForVirksomhet
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForVirksomhetMetadata
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagreITestBucket
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkNøkkel
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class ImportServiceIntegrasjonstest {
    private val gcsContainer = TestContainerHelper.googleCloudStorage
    private val kafkaContainer = TestContainerHelper.kafka
    private val eksportertStatistikkKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER)

    private val eksportertVirksomhetStatistikkKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET)

    private val eksportertVirksomhetMetadataKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET_METADATA)

    @BeforeTest
    fun setup() {
        gcsContainer.opprettTestBucketHvisIkkeFunnet()
        /*
         GCS Rest API er tilgjengelig fra eksponert port (dynamic port) på localhost (kjør test i debug)
         f.eks: http://localhost:{dynamic_port}/storage/v1/b/fake-gcs-bucket-in-container/o/land.json
         */
        eksportertStatistikkKonsument.subscribe(
            mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER.navnMedNamespace),
        )
        eksportertVirksomhetStatistikkKonsument.subscribe(
            mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET.navnMedNamespace),
        )
        eksportertVirksomhetMetadataKonsument.subscribe(
            mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET_METADATA.navnMedNamespace),
        )
    }

    @AfterTest
    fun tearDown() {
        eksportertStatistikkKonsument.unsubscribe()
        eksportertStatistikkKonsument.close()

        eksportertVirksomhetStatistikkKonsument.unsubscribe()
        eksportertVirksomhetStatistikkKonsument.close()

        eksportertVirksomhetMetadataKonsument.unsubscribe()
        eksportertVirksomhetMetadataKonsument.close()
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
                """.trimIndent().encodeToByteArray(),
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = "land.json")
        verifiserBlobFinnes shouldBe true

        kafkaContainer.sendJobbMelding(Jobb.landSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'LAND'".toRegex()
        dvhImportApplikasjon shouldContainLog
            "Fikk exception i import prosess med melding 'Encountered an unknown key 'testField'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'landSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `import statistikk LAND og send statistikk til Kafka`() {
        lagTestDataForLand().lagreITestBucket(
            gcsContainer = gcsContainer,
            kategori = StatistikkKategori.LAND,
            nøkkel = "land",
            verdi = "NO",
        )

        kafkaContainer.sendJobbMelding(Jobb.landSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'LAND'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori LAND er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'landSykefraværsstatistikkDvhImport' ferdig".toRegex()

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                nøkkel = Json.encodeToString(
                    SykefraværsstatistikkNøkkel(
                        årstall = 2024,
                        kvartal = 2,
                        kategori = StatistikkKategori.LAND,
                        kode = "NO",
                    ),
                ),
                konsument = eksportertStatistikkKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<LandSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 1
                deserialiserteSvar.forAtLeastOne { landStatistikk ->
                    landStatistikk.land shouldBe "NO"
                    landStatistikk.årstall shouldBe 2024
                    landStatistikk.kvartal shouldBe 2
                    landStatistikk.tapteDagsverk shouldBe 8894426.768373.toBigDecimal()
                    landStatistikk.muligeDagsverk shouldBe 143458496.063556.toBigDecimal()
                    landStatistikk.antallPersoner shouldBe 3124427
                    landStatistikk.prosent shouldBe 6.2.toBigDecimal()
                }
            }
        }
    }

    @Test
    fun `import statistikk SEKTOR og send statistikk til Kafka`() {
        lagTestDataForSektor(gcsContainer = gcsContainer, årstall = 2024, kvartal = 2)

        kafkaContainer.sendJobbMelding(Jobb.sektorSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'SEKTOR'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori SEKTOR er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'sektorSykefraværsstatistikkDvhImport' ferdig".toRegex()

        val nøkkel: StatistikkKategori = StatistikkKategori.SEKTOR

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                filtreringsnøkkel = nøkkel,
                konsument = eksportertStatistikkKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<SektorSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 2
                deserialiserteSvar.filter { it.sektor == "3" }.forAtLeastOne { sektorStatistikk ->
                    sektorStatistikk.sektor shouldBe "3"
                    sektorStatistikk.årstall shouldBe 2024
                    sektorStatistikk.kvartal shouldBe 2
                    sektorStatistikk.prosent shouldBe 2.7.toBigDecimal()
                    sektorStatistikk.tapteDagsverk shouldBe 94426.768373.toBigDecimal()
                    sektorStatistikk.muligeDagsverk shouldBe 3458496.063556.toBigDecimal()
                    sektorStatistikk.antallPersoner shouldBe 24427
                }
                deserialiserteSvar.filter { it.sektor == "2" }.forAtLeastOne { sektorStatistikk ->
                    sektorStatistikk.sektor shouldBe "2"
                    sektorStatistikk.årstall shouldBe 2024
                    sektorStatistikk.kvartal shouldBe 2
                    sektorStatistikk.prosent shouldBe 6.2.toBigDecimal()
                    sektorStatistikk.tapteDagsverk shouldBe 88944.768373.toBigDecimal()
                    sektorStatistikk.muligeDagsverk shouldBe 1434584.063556.toBigDecimal()
                    sektorStatistikk.antallPersoner shouldBe 3124427
                }
            }
        }
    }

    @Test
    fun `import statistikk NÆRING og send statistikk til Kafka`() {
        lagTestDataForNæring(gcsContainer = gcsContainer, årstall = 2024, kvartal = 2, næring = "02")

        kafkaContainer.sendJobbMelding(Jobb.næringSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'NÆRING'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRING er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'næringSykefraværsstatistikkDvhImport' ferdig".toRegex()

        val nøkkel = StatistikkKategori.NÆRING

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                filtreringsnøkkel = nøkkel,
                konsument = eksportertStatistikkKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<NæringSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 2
                deserialiserteSvar.filter { it.næring == "88" }.forAtLeastOne { næringStatistikk ->
                    næringStatistikk.næring shouldBe "88"
                    næringStatistikk.årstall shouldBe 2024
                    næringStatistikk.kvartal shouldBe 2
                    næringStatistikk.prosent shouldBe 2.7.toBigDecimal()
                    næringStatistikk.tapteDagsverk shouldBe 94426.768373.toBigDecimal()
                    næringStatistikk.muligeDagsverk shouldBe 3458496.063556.toBigDecimal()
                    næringStatistikk.antallPersoner shouldBe 24427
                }
                deserialiserteSvar.filter { it.næring == "02" }.forAtLeastOne { næringStatistikk ->
                    næringStatistikk.næring shouldBe "02"
                    næringStatistikk.årstall shouldBe 2024
                    næringStatistikk.kvartal shouldBe 2
                    næringStatistikk.prosent shouldBe 6.2.toBigDecimal()
                    næringStatistikk.tapteDagsverk shouldBe 88944.768373.toBigDecimal()
                    næringStatistikk.muligeDagsverk shouldBe 1434584.063556.toBigDecimal()
                    næringStatistikk.antallPersoner shouldBe 3124427
                }
            }
        }
    }

    @Test
    fun `import statistikk NÆRINGSKODE`() {
        lagTestDataForNæringskode(gcsContainer = gcsContainer, årstall = 2024, kvartal = 2)

        kafkaContainer.sendJobbMelding(Jobb.næringskodeSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'NÆRINGSKODE'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRINGSKODE er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'næringskodeSykefraværsstatistikkDvhImport' ferdig".toRegex()

        val nøkkel = StatistikkKategori.NÆRINGSKODE
        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                filtreringsnøkkel = nøkkel,
                konsument = eksportertStatistikkKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<NæringskodeSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 2
                deserialiserteSvar.filter { it.næringskode == "88911" }.forAtLeastOne { næringskodeStatistikk ->
                    næringskodeStatistikk.næringskode shouldBe "88911"
                    næringskodeStatistikk.årstall shouldBe 2024
                    næringskodeStatistikk.kvartal shouldBe 2
                    næringskodeStatistikk.prosent shouldBe 2.7.toBigDecimal()
                    næringskodeStatistikk.tapteDagsverk shouldBe 94426.768373.toBigDecimal()
                    næringskodeStatistikk.muligeDagsverk shouldBe 3458496.063556.toBigDecimal()
                    næringskodeStatistikk.tapteDagsverkGradert shouldBe 90.034285.toBigDecimal()
                    næringskodeStatistikk.tapteDagsverkPerVarighet.size shouldBe 1
                    næringskodeStatistikk.tapteDagsverkPerVarighet[0].varighet shouldBe "D"
                    næringskodeStatistikk.tapteDagsverkPerVarighet[0].tapteDagsverk shouldBe 148.534285.toBigDecimal()
                    næringskodeStatistikk.antallPersoner shouldBe 24427
                }
                deserialiserteSvar.filter { it.næringskode == "02300" }.forAtLeastOne { næringskodeStatistikk ->
                    næringskodeStatistikk.næringskode shouldBe "02300"
                    næringskodeStatistikk.årstall shouldBe 2024
                    næringskodeStatistikk.kvartal shouldBe 2
                    næringskodeStatistikk.prosent shouldBe 6.2.toBigDecimal()
                    næringskodeStatistikk.tapteDagsverk shouldBe 88944.768373.toBigDecimal()
                    næringskodeStatistikk.muligeDagsverk shouldBe 1434584.063556.toBigDecimal()
                    næringskodeStatistikk.tapteDagsverkGradert shouldBe 90.034285.toBigDecimal()
                    næringskodeStatistikk.tapteDagsverkPerVarighet.size shouldBe 1
                    næringskodeStatistikk.tapteDagsverkPerVarighet[0].varighet shouldBe "D"
                    næringskodeStatistikk.tapteDagsverkPerVarighet[0].tapteDagsverk shouldBe 148.534285.toBigDecimal()
                    næringskodeStatistikk.antallPersoner shouldBe 3124427
                }
            }
        }
    }

    @Test
    fun `import statistikk VIRKSOMHET`() {
        lagTestDataForVirksomhet(gcsContainer = gcsContainer, "987654321", 2024, 2)

        kafkaContainer.sendJobbMelding(Jobb.virksomhetSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'VIRKSOMHET'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori VIRKSOMHET er: '26.0'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'virksomhetSykefraværsstatistikkDvhImport' ferdig".toRegex()

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                nøkkel = Json.encodeToString(
                    SykefraværsstatistikkNøkkel(
                        årstall = 2024,
                        kvartal = 2,
                        kategori = StatistikkKategori.VIRKSOMHET,
                        kode = "987654321",
                    ),
                ),
                konsument = eksportertVirksomhetStatistikkKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<VirksomhetSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 1
                deserialiserteSvar.filter { it.orgnr == "987654321" }.forAtLeastOne { virksomhetStatistikk ->
                    virksomhetStatistikk.orgnr shouldBe "987654321"
                    virksomhetStatistikk.årstall shouldBe 2024
                    virksomhetStatistikk.kvartal shouldBe 2
                    virksomhetStatistikk.prosent shouldBe 26.0.toBigDecimal()
                    virksomhetStatistikk.tapteDagsverk shouldBe 20.23.toBigDecimal()
                    virksomhetStatistikk.muligeDagsverk shouldBe 77.8716.toBigDecimal()
                    virksomhetStatistikk.tapteDagsverkGradert shouldBe 90.034285.toBigDecimal()
                    virksomhetStatistikk.tapteDagsverkPerVarighet.size shouldBe 6
                    virksomhetStatistikk.tapteDagsverkPerVarighet[0].varighet shouldBe "A"
                    virksomhetStatistikk.tapteDagsverkPerVarighet[0].tapteDagsverk shouldBe 12.1527.toBigDecimal()
                    virksomhetStatistikk.antallPersoner shouldBe 40
                    virksomhetStatistikk.rectype shouldBe "1"
                }
            }
        }
    }

    @Test
    fun `import statistikk VIRKSOMHET_METADATA`() {
        lagTestDataForVirksomhetMetadata(gcsContainer = gcsContainer)

        kafkaContainer.sendJobbMelding(Jobb.virksomhetMetadataSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av virksomhet metadata".toRegex()
        dvhImportApplikasjon shouldContainLog "Antall metadata prosessert for kategori VIRKSOMHET_METADATA er: '1'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'virksomhetMetadataSykefraværsstatistikkDvhImport' ferdig".toRegex()

        val nøkkel = Json.encodeToString(
            EksportProdusent.VirksomhetMetadataNøkkel(
                årstall = 2024,
                kvartal = 2,
                orgnr = "987654321",
            ),
        )
        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                nøkkel = nøkkel,
                konsument = eksportertVirksomhetMetadataKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<VirksomhetMetadataDto>(it)
                }

                deserialiserteSvar shouldHaveAtLeastSize 1
                deserialiserteSvar.filter { it.orgnr == "987654321" }.forAtLeastOne { virksomhetMetadataStatistikk ->
                    virksomhetMetadataStatistikk.orgnr shouldBe "987654321"
                    virksomhetMetadataStatistikk.årstall shouldBe 2024
                    virksomhetMetadataStatistikk.kvartal shouldBe 2
                    virksomhetMetadataStatistikk.sektor shouldBe "2"
                    virksomhetMetadataStatistikk.primærnæring shouldBe "88"
                    virksomhetMetadataStatistikk.primærnæringskode shouldBe "88911"
                    virksomhetMetadataStatistikk.rectype shouldBe "1"
                }
            }
        }
    }

    @Test
    fun `primærnæring og primærnæringskode i VIRKSOMHET_METADATA kan være null`() {
        lagTestDataForVirksomhetMetadata(gcsContainer = gcsContainer, primærnæring = null, primærnæringskode = null)

        kafkaContainer.sendJobbMelding(Jobb.virksomhetMetadataSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av virksomhet metadata".toRegex()
        dvhImportApplikasjon shouldContainLog "Antall metadata prosessert for kategori VIRKSOMHET_METADATA er: '1'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'virksomhetMetadataSykefraværsstatistikkDvhImport' ferdig".toRegex()

        val nøkkel = Json.encodeToString(
            EksportProdusent.VirksomhetMetadataNøkkel(
                årstall = 2024,
                kvartal = 2,
                orgnr = "987654321",
            ),
        )
        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                nøkkel = nøkkel,
                konsument = eksportertVirksomhetMetadataKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<VirksomhetMetadataDto>(it)
                }

                deserialiserteSvar shouldHaveAtLeastSize 1
                deserialiserteSvar.filter { it.orgnr == "987654321" }.forAtLeastOne { virksomhetMetadataStatistikk ->
                    virksomhetMetadataStatistikk.orgnr shouldBe "987654321"
                    virksomhetMetadataStatistikk.årstall shouldBe 2024
                    virksomhetMetadataStatistikk.kvartal shouldBe 2
                    virksomhetMetadataStatistikk.sektor shouldBe "2"
                    virksomhetMetadataStatistikk.primærnæring shouldBe null
                    virksomhetMetadataStatistikk.primærnæringskode shouldBe null
                    virksomhetMetadataStatistikk.rectype shouldBe "1"
                }
            }
        }
    }

    @Test
    fun `import statistikk for alle kategorier`() {
        lagTestDataForLand().lagreITestBucket(
            gcsContainer = gcsContainer,
            kategori = StatistikkKategori.LAND,
            nøkkel = "land",
            verdi = "NO",
        )
        lagTestDataForSektor(gcsContainer = gcsContainer)
        lagTestDataForNæring(gcsContainer = gcsContainer, årstall = 2024, næring = "02")
        lagTestDataForNæringskode(gcsContainer = gcsContainer)
        lagTestDataForVirksomhet(gcsContainer = gcsContainer, orgnr = "987654321", årstall = 2024, kvartal = 2)
        lagTestDataForVirksomhetMetadata(gcsContainer = gcsContainer)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for alle statistikkkategorier".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori LAND er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori SEKTOR er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRING er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRINGSKODE er: '3.7'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori VIRKSOMHET er: '26.0'".toRegex()
        dvhImportApplikasjon shouldContainLog "Antall metadata prosessert for kategori VIRKSOMHET_METADATA er: '1'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'alleKategorierSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }
}
