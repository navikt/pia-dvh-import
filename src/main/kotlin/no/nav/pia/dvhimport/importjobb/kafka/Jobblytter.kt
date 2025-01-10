package no.nav.pia.dvhimport.importjobb.kafka

import ia.felles.integrasjoner.jobbsender.Jobb
import ia.felles.integrasjoner.jobbsender.Jobb.alleKategorierSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.landSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.næringSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.næringskodeSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.publiseringsdatoDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.sektorSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.virksomhetMetadataSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.virksomhetSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.JobbInfo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.importjobb.ImportService
import no.nav.pia.dvhimport.importjobb.domene.DvhMetadata
import no.nav.pia.dvhimport.importjobb.domene.DvhStatistikkKategori
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class Jobblytter(
    val importService: ImportService,
) : CoroutineScope {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val job: Job = Job()
    private val topic = KafkaTopics.PIA_JOBBLYTTER
    private val kafkaConsumer = KafkaConsumer(
        KafkaConfig().consumerProperties(konsumentGruppe = topic.konsumentGruppe),
        StringDeserializer(),
        StringDeserializer(),
    )

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + job

    init {
        Runtime.getRuntime().addShutdownHook(Thread(this::cancel))
    }

    fun run() {
        launch {
            kafkaConsumer.use { consumer ->
                try {
                    consumer.subscribe(listOf(topic.navnMedNamespace))
                    logger.info("Kafka consumer subscribed to ${topic.navnMedNamespace}")
                    while (job.isActive) {
                        val records = consumer.poll(Duration.ofSeconds(1))
                        records.forEach {
                            val jobbInfo = Json.decodeFromString<SerializableJobbInfo>(it.value())
                            if (jobbInfo.jobb.name != it.key()) {
                                logger.warn(
                                    "Received record with key ${it.key()} and value ${it.value()} from topic ${it.topic()} but jobInfo.job is ${jobbInfo.jobb}",
                                )
                            } else {
                                logger.info("Starter jobb ${jobbInfo.jobb}")
                                when (jobbInfo.jobb) {
                                    alleKategorierSykefraværsstatistikkDvhImport -> {
                                        importService.importAlleStatistikkKategorier()
                                    }
                                    landSykefraværsstatistikkDvhImport -> {
                                        importService.importForStatistikkKategori(DvhStatistikkKategori.LAND)
                                    }
                                    sektorSykefraværsstatistikkDvhImport -> {
                                        importService.importForStatistikkKategori(DvhStatistikkKategori.SEKTOR)
                                    }
                                    næringSykefraværsstatistikkDvhImport -> {
                                        importService.importForStatistikkKategori(DvhStatistikkKategori.NÆRING)
                                    }
                                    næringskodeSykefraværsstatistikkDvhImport -> {
                                        importService.importForStatistikkKategori(DvhStatistikkKategori.NÆRINGSKODE)
                                    }
                                    virksomhetSykefraværsstatistikkDvhImport -> {
                                        importService.importForStatistikkKategori(DvhStatistikkKategori.VIRKSOMHET)
                                    }
                                    virksomhetMetadataSykefraværsstatistikkDvhImport -> {
                                        importService.importMetadata(DvhMetadata.VIRKSOMHET_METADATA)
                                    }
                                    publiseringsdatoDvhImport -> {
                                        importService.importMetadata(DvhMetadata.PUBLISERINGSDATO)
                                    }
                                    else -> {
                                        logger.info("Jobb '${jobbInfo.jobb}' ignorert")
                                    }
                                }
                                logger.info("Jobb '${jobbInfo.jobb}' ferdig")
                            }
                        }
                        consumer.commitSync()
                    }
                } catch (e: WakeupException) {
                    logger.info("Jobblytter is shutting down")
                } catch (e: RetriableException) {
                    logger.error("Kafka consumer got retriable exception", e)
                } catch (e: Exception) {
                    logger.error("Exception is shutting down kafka listner for ${topic.navnMedNamespace}", e)
                    throw e
                }
            }
        }
    }

    @Serializable
    data class SerializableJobbInfo(
        override val jobb: Jobb,
        override val tidspunkt: String,
        override val applikasjon: String,
        override val parameter: String?,
    ) : JobbInfo

    private fun cancel() =
        runBlocking {
            logger.info("Stopping kafka consumer job for ${topic.navn}")
            kafkaConsumer.wakeup()
            job.cancelAndJoin()
            logger.info("Stopped kafka consumer job for ${topic.navn}")
        }
}
