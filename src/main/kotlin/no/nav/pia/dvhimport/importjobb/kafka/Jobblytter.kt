package no.nav.pia.dvhimport.importjobb.kafka

import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import no.nav.fia.arbeidsgiver.konfigurasjon.KafkaTopics
import no.nav.pia.dvhimport.importjobb.domene.StatistikkImportService
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class Jobblytter(val statistikkImportService: StatistikkImportService) : CoroutineScope {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val job: Job = Job()
    private val topic = KafkaTopics.DVH_IMPORT_JOBBLYTTER
    private val kafkaConsumer = KafkaConsumer(
        KafkaConfig().consumerProperties(konsumentGruppe = topic.konsumentGruppe),
        StringDeserializer(),
        StringDeserializer()
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
                        logger.info("[DEBUG] fikk ${records.count()} meldinger")
                        records.forEach {
                            logger.info("[DEBUG] nøkkel ${it.key()}")
                            val jobInfo = Json.decodeFromString<JobInfo>(it.value())
                            if (jobInfo.jobb != it.key())
                                logger.warn("Received record with key ${it.key()} and value ${it.value()} from topic ${it.topic()} but jobInfo.job is ${jobInfo.jobb}")
                            else {
                                logger.info("Starter jobb ${jobInfo.jobb}")
                                when (jobInfo.jobb) {
                                    "importSykefraværKvartalsstatistikk" -> {
                                        statistikkImportService.start()
                                    }

                                    else -> {
                                        logger.info("Jobb '${jobInfo.jobb}' ignorert")
                                    }
                                }
                                logger.info("Jobb '${jobInfo.jobb}' ferdig")
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


    private fun cancel() = runBlocking {
        logger.info("Stopping kafka consumer job for ${topic.navn}")
        kafkaConsumer.wakeup()
        job.cancelAndJoin()
        logger.info("Stopped kafka consumer job for ${topic.navn}")
    }
}

@Serializable
data class JobInfo(
    val jobb: String,
    val tidspunkt: String,
    val applikasjon: String,
)
