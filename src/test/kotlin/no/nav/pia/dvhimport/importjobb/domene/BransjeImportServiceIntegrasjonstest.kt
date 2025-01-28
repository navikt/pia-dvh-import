package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.definisjoner.bransjer.Bransje
import ia.felles.definisjoner.bransjer.BransjeId
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
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæring
import no.nav.pia.dvhimport.helper.TestDataGenerator.Companion.lagTestDataForNæringskode
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkNøkkel
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
                nøkkel = Json.encodeToString<SykefraværsstatistikkNøkkel>(
                    SykefraværsstatistikkNøkkel(
                        årstall = 2024,
                        kvartal = 2,
                        kategori = StatistikkKategori.BRANSJE,
                        kode = "Barnehager",
                    ),
                ),
                konsument = eksportertPubliseringsdatoKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<BransjeSykefraværsstatistikkDto>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 1
                deserialiserteSvar.filter { it.bransje == Bransje.BARNEHAGER.navn }.forAtLeastOne { bransjeStatistikk ->
                    bransjeStatistikk.bransje shouldBe Bransje.BARNEHAGER.navn
                    bransjeStatistikk.årstall shouldBe 2024
                    bransjeStatistikk.kvartal shouldBe 2
                    bransjeStatistikk.prosent shouldBe 2.7.toBigDecimal()
                    bransjeStatistikk.tapteDagsverk shouldBe 94426.768373.toBigDecimal()
                    bransjeStatistikk.muligeDagsverk shouldBe 3458496.063556.toBigDecimal()
                    bransjeStatistikk.tapteDagsverkGradert shouldBe 90.034285.toBigDecimal()
                    bransjeStatistikk.tapteDagsverkPerVarighet.size shouldBe 1
                    bransjeStatistikk.tapteDagsverkPerVarighet[0].varighet shouldBe "D"
                    bransjeStatistikk.tapteDagsverkPerVarighet[0].tapteDagsverk shouldBe 148.534285.toBigDecimal()
                    bransjeStatistikk.antallPersoner shouldBe 24427
                }
            }
        }
    }
}
