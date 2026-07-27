package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.integrasjoner.jobbsender.Jobb
import io.kotest.inspectors.forAtLeastOne
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.maps.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.KonsistentTestdata
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForLand
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæring
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæringskode
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForSektor
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForVirksomhet
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForVirksomhetMetadata
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagreITestBucket
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.DatavarehusRecordType
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkNøkkel
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportLockRepository
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportLockStatus
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportSteg
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportStegRepository
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportStegStatus
import no.nav.pia.dvhimport.importjobb.orkestrering.Kontroll
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import no.nav.pia.dvhimport.konfigurasjon.createDataSource
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import java.math.BigDecimal
import java.time.LocalDate
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using

class ImportServiceIntegrasjonstest {
    private val gcsContainer = TestContainerHelper.googleCloudStorage
    private val kafkaContainer = TestContainerHelper.kafka

    companion object {
        // Kobler til SAMME postgres som appen, via host-tilgjengelig jdbcUrl (appen bruker nettverks-alias).
        private val testDataSource = createDataSource(
            TestContainerHelper.postgresContainerHelper.container.jdbcUrl +
                "&user=${TestContainerHelper.postgresContainerHelper.container.username}" +
                "&password=${TestContainerHelper.postgresContainerHelper.container.password}",
        )
        private val publiseringsdatoRepository = PubliseringsdatoRepository(testDataSource)
        private val lockRepository = ImportLockRepository(testDataSource)
        private val stegRepository = ImportStegRepository(testDataSource)
    }
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
            "Import feilet for kategori 'LAND'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'landSykefraværsstatistikkDvhImport' feilet".toRegex()
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
                    virksomhetStatistikk.rectype shouldBe DatavarehusRecordType.UNDERENHET.kode
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
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2026, kvartal = 2)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2026-2")

        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori LAND er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori SEKTOR er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRING er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRINGSKODE er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori VIRKSOMHET er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Import ferdig for 2. kvartal 2026".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'alleKategorierSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `dry-run validerer alle steg uten å endre DB eller sende Kafka`() {
        val årstall = 2027
        val kvartal = 1
        val dato = LocalDate.of(2027, 3, 1)
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        val id = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!

        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2027-1:DRY_RUN")

        dvhImportApplikasjon shouldContainLog "DRY_RUN: validering fullført for 1. kvartal 2027".toRegex()

        // Dry-run persisterer ingenting: ingen lås, ingen steg, publiseringsdato fortsatt uprosessert
        lockRepository.hentForPubliseringsdato(id) shouldBe null
        stegRepository.hentAlle(id).shouldBeEmpty()
        publiseringsdatoRepository.hentUprosessertForDato(dato).shouldNotBeNull()
    }

    @Test
    fun `dry-run rapporterer valideringsfeil uten å endre DB eller sende Kafka`() {
        val årstall = 2027
        val kvartal = 2
        val dato = LocalDate.of(2027, 6, 1)
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        val id = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!

        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal)
        // Overstyr sektor slik at aggregert sf_prosent avviker fra LAND (6.2)
        KonsistentTestdata.skrivSektor(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal, tapteDagsverk = "30")

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2027-2:DRY_RUN")

        dvhImportApplikasjon shouldContainLog "DRY_RUN: validering FEILET på steg IMPORT_SEKTOR \\(SF_PROSENT_FEIL\\)".toRegex()

        // Ingen bivirkninger — heller ikke på feil-stien (motsatt av ekte kjøring som ville skrevet lås+steg FEILET)
        lockRepository.hentForPubliseringsdato(id) shouldBe null
        stegRepository.hentAlle(id).shouldBeEmpty()
        publiseringsdatoRepository.hentUprosessertForDato(dato).shouldNotBeNull()
    }

    @Test
    fun `orkestrert import er deterministisk over gjentatte kjøringer med samme data`() {
        val årstall = 2028
        val kvartal = 1
        val dato = LocalDate.of(2028, 3, 1)
        val antallVirksomheter = 1200
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        val id = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!

        KonsistentTestdata.skrivAlleKonsistenteFilerMedVolum(
            gcsContainer = gcsContainer,
            årstall = årstall,
            kvartal = kvartal,
            antallVirksomheter = antallVirksomheter,
        )

        // Kjør samme kvartal 3 ganger med sletting mellom hver kjøring, og fang sluttilstanden per steg.
        val snapshots = (1..3).map {
            kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2028-1")
            ventPåLåsStatus(id, ImportLockStatus.FERDIG, timeoutSekunder = 60)
            val snapshot = stegRepository.hentAlle(id).associate { steg ->
                steg.steg to StegResultat(steg.status, steg.antallRaderLest, steg.antallSendtPaaKafka, steg.sfProsent)
            }
            nullstillOrkestrering(id)
            snapshot
        }

        // Determinisme: alle tre kjøringene gav identisk resultat per steg (status, rader lest, sendt, sf_prosent)
        snapshots[1] shouldBe snapshots[0]
        snapshots[2] shouldBe snapshots[0]

        val referanse = snapshots[0]
        referanse shouldHaveSize 7
        referanse.values.all { it.status == ImportStegStatus.FERDIG } shouldBe true
        referanse.getValue(ImportSteg.IMPORT_VIRKSOMHET).antallRaderLest shouldBe antallVirksomheter
        referanse.getValue(ImportSteg.IMPORT_VIRKSOMHET_METADATA).antallRaderLest shouldBe antallVirksomheter
        referanse.getValue(ImportSteg.IMPORT_LAND).sfProsent.shouldNotBeNull()
    }

    @Test
    fun `virksomheter med ugyldig orgnr filtreres bort og sendes ikke til Kafka`() {
        val årstall = 2029
        val kvartal = 1
        val dato = LocalDate.of(2029, 3, 1)
        val antallGyldige = 100
        // 7-sifrede orgnr = kjent støy i virksomhet.json; skal filtreres av struktur-regexen ^\d{9}$
        val ugyldigeOrgnr = listOf("1234567", "7654321", "9999999", "1112223", "3334445")
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        val id = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!

        val alleOrgnr = KonsistentTestdata.volumOrgnr(antallGyldige) + ugyldigeOrgnr
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal)
        KonsistentTestdata.skrivVirksomhet(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal, orgnr = alleOrgnr)
        KonsistentTestdata.skrivMetadata(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal, orgnr = alleOrgnr)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2029-1")
        ventPåLåsStatus(id, ImportLockStatus.FERDIG, timeoutSekunder = 60)

        val steg = stegRepository.hentAlle(id).associateBy { it.steg }
        val virksomhet = steg.getValue(ImportSteg.IMPORT_VIRKSOMHET)
        val metadata = steg.getValue(ImportSteg.IMPORT_VIRKSOMHET_METADATA)

        virksomhet.antallRaderLest shouldBe antallGyldige
        metadata.antallRaderLest shouldBe antallGyldige

        virksomhet.antallSendtPaaKafka shouldBe antallGyldige
        metadata.antallSendtPaaKafka shouldBe antallGyldige
    }

    private data class StegResultat(
        val status: ImportStegStatus,
        val antallRaderLest: Int,
        val antallSendtPaaKafka: Int,
        val sfProsent: BigDecimal?,
    )

    /**
     * Sletter orkestrerings-sporene for et kvartal slik at samme import kan kjøres på nytt fra bunnen
     * (lås = null => taLås). Brukes mellom iterasjonene i determinisme-testen.
     */
    private fun nullstillOrkestrering(publiseringsdatoId: Int) {
        using(sessionOf(testDataSource)) { session ->
            session.run(
                queryOf(
                    "DELETE FROM automatisering_import_steg WHERE publiseringsdato_id = :id",
                    mapOf("id" to publiseringsdatoId),
                ).asUpdate,
            )
            session.run(
                queryOf(
                    "DELETE FROM automatisering_import_lock WHERE publiseringsdato_id = :id",
                    mapOf("id" to publiseringsdatoId),
                ).asUpdate,
            )
            session.run(
                queryOf(
                    "UPDATE publiseringsdato SET prosessert = false WHERE id = :id",
                    mapOf("id" to publiseringsdatoId),
                ).asUpdate,
            )
        }
    }

    @Test
    fun `import feiler når sf_prosent for en kategori avviker fra LAND`() {
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2026, kvartal = 3)
        // Overstyr sektor slik at aggregert sf_prosent (3.0) avviker fra LAND (6.2)
        KonsistentTestdata.skrivSektor(gcsContainer = gcsContainer, årstall = 2026, kvartal = 3, tapteDagsverk = "30")

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2026-3")

        verifiserFeilet(
            årstall = 2026,
            kvartal = 3,
            feilendeSteg = ImportSteg.IMPORT_SEKTOR,
            forventetKontroll = Kontroll.SF_PROSENT_FEIL,
        )
        dvhImportApplikasjon shouldContainLog "avviker fra referanse".toRegex()
    }

    @Test
    fun `import feiler ved feil struktur i næringskode`() {
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2026, kvartal = 1)
        // Næringskode med 10 siffer bryter strukturkravet ^\d{5}$
        KonsistentTestdata.skrivNæringskode(gcsContainer = gcsContainer, årstall = 2026, kvartal = 1, næringskoder = listOf("0111012345"))

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2026-1")

        verifiserFeilet(
            årstall = 2026,
            kvartal = 1,
            feilendeSteg = ImportSteg.IMPORT_NARINGSKODE,
            forventetKontroll = Kontroll.FEIL_STRUKTUR_I_INPUT_FIL,
        )
        dvhImportApplikasjon shouldContainLog "bryter strukturkravet".toRegex()
    }

    @Test
    fun `import feiler når årstall i data ikke matcher jobben`() {
        // Data for 2024, men jobben ber om 2025-4
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2024, kvartal = 4)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2025-4")

        verifiserFeilet(
            årstall = 2025,
            kvartal = 4,
            feilendeSteg = ImportSteg.IMPORT_LAND,
            forventetKontroll = Kontroll.FEIL_ÅRSTALL_ELLER_KVARTAL,
        )
        dvhImportApplikasjon shouldContainLog "feil årstall/kvartal".toRegex()
    }

    /**
     * Verifiserer at orkestreringen endte i riktig feiltilstand:
     *  - låsen er FEILET
     *  - det feilende steget er FEILET med riktig kontroll
     *  - INGEN steg er FERDIG => sendefasen kjørte aldri => ingen Kafka sendt (to-fase-garantien)
     */
    private fun verifiserFeilet(
        årstall: Int,
        kvartal: Int,
        feilendeSteg: ImportSteg,
        forventetKontroll: Kontroll,
    ) {
        val publiseringsdatoId = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!
        ventPåLåsStatus(publiseringsdatoId, ImportLockStatus.FEILET)

        val steg = stegRepository.hentAlle(publiseringsdatoId)
        val feilet = steg.first { it.steg == feilendeSteg }
        feilet.status shouldBe ImportStegStatus.FEILET
        feilet.kontroll shouldBe forventetKontroll
        steg.none { it.status == ImportStegStatus.FERDIG } shouldBe true
    }

    private fun ventPåLåsStatus(
        publiseringsdatoId: Int,
        status: ImportLockStatus,
        timeoutSekunder: Long = 20,
    ) {
        val slutt = System.currentTimeMillis() + timeoutSekunder * 1000
        while (System.currentTimeMillis() < slutt) {
            if (lockRepository.hentForPubliseringsdato(publiseringsdatoId)?.status == status) return
            Thread.sleep(100)
        }
        throw AssertionError("Lås for publiseringsdato $publiseringsdatoId ble ikke $status innen $timeoutSekunder sek")
    }
}
