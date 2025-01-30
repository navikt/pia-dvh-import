package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.definisjoner.bransjer.Bransje
import ia.felles.definisjoner.bransjer.BransjeId
import ia.felles.integrasjoner.jobbsender.Jobb
import io.kotest.inspectors.forAtLeastOne
import io.kotest.matchers.collections.shouldHaveAtLeastSize
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæring
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæringskode
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæringskodeSykehjem
import no.nav.pia.dvhimport.helper.TestUtils.Companion.bigDecimalShouldBe
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class BransjeImportServiceIntegrasjonstest {
    private val gcsContainer = TestContainerHelper.googleCloudStorage
    private val kafkaContainer = TestContainerHelper.kafka
    private val eksportertPubliseringsdatoKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER)

    @BeforeTest
    fun setup() {
        gcsContainer.opprettTestBucketHvisIkkeFunnet()
        eksportertPubliseringsdatoKonsument.subscribe(
            mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER.navnMedNamespace),
        )
    }

    @AfterTest
    fun tearDown() {
        eksportertPubliseringsdatoKonsument.unsubscribe()
        eksportertPubliseringsdatoKonsument.close()
    }

    @Test
    fun `import bransjer og send melding til Kafka`() {
        lagTestDataForNæring(
            gcsContainer = gcsContainer,
            årstall = 2024,
            kvartal = 2,
            næring = (Bransje.BYGG.bransjeId as BransjeId.Næring).næring,
        )
        lagTestDataForNæringskode(gcsContainer = gcsContainer) // inneholder BARNEHAGER

        kafkaContainer.sendJobbMelding(Jobb.bransjeSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'BRANSJE'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'bransjeSykefraværsstatistikkDvhImport' ferdig".toRegex()

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                filtreringsnøkkel = StatistikkKategori.BRANSJE,
                konsument = eksportertPubliseringsdatoKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<BransjeSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveSize 2
                deserialiserteSvar.filter { it.bransje == Bransje.BARNEHAGER.navn }.forAtLeastOne { bransjeStatistikk ->
                    bransjeStatistikk.bransje shouldBe Bransje.BARNEHAGER.navn
                    bransjeStatistikk.årstall shouldBe 2024
                    bransjeStatistikk.kvartal shouldBe 2
                    bransjeStatistikk.prosent bigDecimalShouldBe 2.7
                    bransjeStatistikk.tapteDagsverk bigDecimalShouldBe 94426.768373
                    bransjeStatistikk.muligeDagsverk bigDecimalShouldBe 3458496.063556
                    bransjeStatistikk.tapteDagsverkGradert bigDecimalShouldBe 90.034285
                    bransjeStatistikk.tapteDagsverkPerVarighet.size shouldBe 1
                    bransjeStatistikk.tapteDagsverkPerVarighet[0].varighet shouldBe "D"
                    bransjeStatistikk.tapteDagsverkPerVarighet[0].tapteDagsverk bigDecimalShouldBe 148.534285
                    bransjeStatistikk.antallPersoner shouldBe 24427
                }
                deserialiserteSvar.filter { it.bransje == Bransje.BYGG.navn }.forAtLeastOne { bransjeStatistikk ->
                    bransjeStatistikk.bransje shouldBe Bransje.BYGG.navn
                    bransjeStatistikk.årstall shouldBe 2024
                    bransjeStatistikk.kvartal shouldBe 2
                    bransjeStatistikk.prosent bigDecimalShouldBe 6.2
                    bransjeStatistikk.tapteDagsverk bigDecimalShouldBe 88944.768373
                    bransjeStatistikk.muligeDagsverk bigDecimalShouldBe 1434584.063556
                    bransjeStatistikk.tapteDagsverkGradert bigDecimalShouldBe 90.034285
                    bransjeStatistikk.tapteDagsverkPerVarighet.size shouldBe 1
                    bransjeStatistikk.tapteDagsverkPerVarighet[0].varighet shouldBe "D"
                    bransjeStatistikk.tapteDagsverkPerVarighet[0].tapteDagsverk bigDecimalShouldBe 148.534285
                    bransjeStatistikk.antallPersoner shouldBe 3124427
                }
            }
        }
    }

    @Test
    fun `kan utledde statistikk for bransjer med flere næringskoder`() {
        /*SYKEHJEM(
            navn = "Sykehjem",
            bransjeId = BransjeId.Næringskoder("87101", "87102"),  --> er ikke med: 87103
        ),*/

        lagTestDataForNæringskodeSykehjem(gcsContainer = gcsContainer) // inneholder BARNEHAGER

        kafkaContainer.sendJobbMelding(Jobb.bransjeSykefraværsstatistikkDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for kategori 'BRANSJE'".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'bransjeSykefraværsstatistikkDvhImport' ferdig".toRegex()

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                filtreringsnøkkel = StatistikkKategori.BRANSJE,
                konsument = eksportertPubliseringsdatoKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<BransjeSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 1
                deserialiserteSvar.filter { it.bransje == Bransje.SYKEHJEM.navn }.forAtLeastOne { bransjeStatistikk ->
                    bransjeStatistikk.bransje shouldBe Bransje.SYKEHJEM.navn
                    bransjeStatistikk.årstall shouldBe 2024
                    bransjeStatistikk.kvartal shouldBe 2
                    bransjeStatistikk.prosent bigDecimalShouldBe 10.0
                    bransjeStatistikk.tapteDagsverk bigDecimalShouldBe 301.0
                    bransjeStatistikk.muligeDagsverk bigDecimalShouldBe 3021.0
                    bransjeStatistikk.tapteDagsverkGradert bigDecimalShouldBe 101.0
                    bransjeStatistikk.tapteDagsverkPerVarighet.size shouldBe 2
                    bransjeStatistikk.tapteDagsverkPerVarighet[0].varighet shouldBe "B"
                    bransjeStatistikk.tapteDagsverkPerVarighet[0].tapteDagsverk bigDecimalShouldBe 3.54444
                    bransjeStatistikk.tapteDagsverkPerVarighet[1].varighet shouldBe "D"
                    bransjeStatistikk.tapteDagsverkPerVarighet[1].tapteDagsverk bigDecimalShouldBe 19.1
                    bransjeStatistikk.antallPersoner shouldBe 600
                }
            }
        }
    }
}
