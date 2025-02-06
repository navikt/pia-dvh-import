package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.integrasjoner.jobbsender.Jobb
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.PubliseringsdatoNøkkel
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class PubliseringsdatoImportServiceIntegrasjonstest {
    private val gcsContainer = TestContainerHelper.googleCloudStorage
    private val kafkaContainer = TestContainerHelper.kafka
    private val eksportertPubliseringsdatoKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_PUBLISERINGSDATO)

    @BeforeTest
    fun setup() {
        gcsContainer.opprettTestBucketHvisIkkeFunnet()
        eksportertPubliseringsdatoKonsument.subscribe(
            mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_PUBLISERINGSDATO.navnMedNamespace),
        )
    }

    @AfterTest
    fun tearDown() {
        eksportertPubliseringsdatoKonsument.unsubscribe()
        eksportertPubliseringsdatoKonsument.close()
    }

    @Test
    fun `import publiseringsdato og send melding til Kafka`() {
        lagreTestDataITestBucket()

        kafkaContainer.sendJobbMelding(Jobb.publiseringsdatoDvhImport)

        dvhImportApplikasjon shouldContainLog "Starter import av publiseringsdato".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'publiseringsdatoDvhImport' ferdig".toRegex()

        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                nøkkel = Json.encodeToString(
                    PubliseringsdatoNøkkel(
                        årstall = 2024,
                        rapportPeriode = "202403",
                    ),
                ),
                konsument = eksportertPubliseringsdatoKonsument,
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<PubliseringsdatoDto>(it)
                }
                deserialiserteSvar shouldHaveSize 1
                val publiseringTredjeKvartal = deserialiserteSvar.first { publiseringsdato ->
                    publiseringsdato.rapportPeriode == "202403"
                }

                publiseringTredjeKvartal.rapportPeriode shouldBe "202403"
                publiseringTredjeKvartal.offentligDato shouldBe LocalDateTime.parse("2024-11-28T08:00:00")
                publiseringTredjeKvartal.offentligDato shouldBe LocalDateTime.parse("2024-11-28T08:00") // legit ISO-8601 format
                publiseringTredjeKvartal.offentligDato.time shouldBe LocalTime.parse("08:00:00")
                publiseringTredjeKvartal.oppdatertIDvh shouldBe LocalDateTime.parse("2023-10-20T11:32:59")
            }
        }
    }

    fun lagreTestDataITestBucket() {
        val json =
            """
            [
              {
                "rapport_periode": "202403",
                "offentlig_dato": "2024-11-28, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20, 11:32:59"
               },
              {
                "rapport_periode": "202402",
                "offentlig_dato": "2024-09-05, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20, 11:32:59"
               },
              {
                "rapport_periode": "202401",
                "offentlig_dato": "2024-05-30, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20, 11:32:59"
               },
                 {
                "rapport_periode": "202304",
                "offentlig_dato": "2024-02-29, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20, 11:32:59"
               }
            ]
            """.trimIndent()

        val filnavn = "publiseringsdato.json"
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
            bytes = json.encodeToByteArray(),
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
        verifiserBlobFinnes shouldBe true
    }
}
