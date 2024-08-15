package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.integrasjoner.jobbsender.Jobb
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
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
        eksportertPubliseringsdatoKonsument.subscribe(mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_PUBLISERINGSDATO.navnMedNamespace))
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

        /*
        runBlocking {
            kafkaContainer.ventOgKonsumerKafkaMeldinger(
                key = """{"kvartal":"2024K1","meldingType":"PUBLISERINGSDATO"}""",
                konsument = eksportertPubliseringsdatoKonsument
            ) { meldinger ->
                val deserialiserteSvar = meldinger.map {
                    Json.decodeFromString<Publiseringsdato>(it)
                }
                deserialiserteSvar shouldHaveAtLeastSize 1
                deserialiserteSvar.forAtLeastOne { publiseringsdato ->
                    publiseringsdato.rapportPeriode shouldBe "202401"
                }
            }
        }*/
    }

    data class Publiseringsdato(
        val rapportPeriode: String,
        val offentligDato: String,
        val oppdatertIDvh: String,
    )

    fun lagreTestDataITestBucket() {
        val json = """
            [
              {
                "rapport_periode": "202403",
                "offentlig_dato": "2024-11-28, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20"
               },
              {
                "rapport_periode": "202402",
                "offentlig_dato": "2024-09-05, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20"
               },
              {
                "rapport_periode": "202401",
                "offentlig_dato": "2024-05-30, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20"
               },
                 {
                "rapport_periode": "202304",
                "offentlig_dato": "2024-02-29, 08:00:00",  
                "oppdatert_i_dvh": "2023-10-20"
               },
            ]
        """.trimIndent()

        val filnavn = "publiseringsdato.json"
        gcsContainer.lagreTestBlob(
            blobNavn = filnavn,
            bytes = json.encodeToByteArray()
        )

        val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
        verifiserBlobFinnes shouldBe true

    }
}