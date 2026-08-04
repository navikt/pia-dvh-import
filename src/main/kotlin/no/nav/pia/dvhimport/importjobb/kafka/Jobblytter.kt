package no.nav.pia.dvhimport.importjobb.kafka

import ia.felles.integrasjoner.jobbsender.Jobb
import ia.felles.integrasjoner.jobbsender.Jobb.alleKategorierSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.publiseringsdatoDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.sjekkPubliseringsdatoOgImporter
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
import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportOrkestrering
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
    val importOrkestrering: ImportOrkestrering,
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
                                    "Mottok melding fra topic ${it.topic()} med nøkkel ${it.key()}, " +
                                        "men jobbInfo.jobb er ${jobbInfo.jobb}. " +
                                        "Starter ikke jobb (Kafka-meldingen committes).",
                                )
                            } else {
                                logger.info(
                                    "Starter jobb '${jobbInfo.jobb}' på tidspunkt '${jobbInfo.tidspunkt}' " +
                                        "for applikasjon '${jobbInfo.applikasjon}' " +
                                        "med parameter '${jobbInfo.parameter}'",
                                )
                                try {
                                    when (jobbInfo.jobb) {
                                        // Manuell start av import for alle kategorier statistikk
                                        alleKategorierSykefraværsstatistikkDvhImport -> {
                                            val årstallOgKvartal = jobbInfo.tilÅrstallOgKvartal() ?: run {
                                                logger.warn(
                                                    "Jobb '${jobbInfo.jobb}' krever årstallOgKvartal-parameter, men ingen ble gitt. " +
                                                        "Starter ikke jobb (Kafka-meldingen committes).",
                                                )
                                                return@forEach
                                            }
                                            importOrkestrering.kjørImportForKvartal(
                                                årstallOgKvartal = årstallOgKvartal,
                                                dryRun = jobbInfo.tilDryRun(),
                                            )
                                        }

                                        // Scheduled job [daglig, kl. 21:00] for å hente publiseringsdatoer fra DVH, lagre i DB og sende vider til Kafka
                                        publiseringsdatoDvhImport -> {
                                            importService.importPubliseringsdatoer(
                                                dryRun = jobbInfo.tilDryRun(),
                                            )
                                        }

                                        // Scheduled job [daglig, kl. 08:05] for å sjekke om det er publiseringsdato i dag, og kjøre import for kvartalet hvis det er
                                        sjekkPubliseringsdatoOgImporter -> {
                                            importOrkestrering.kjørImportForPubliseringsdato(
                                                dryRun = jobbInfo.tilDryRun(),
                                            )
                                        }

                                        else -> {
                                            logger.info(
                                                "Jobb '${jobbInfo.jobb}' ignorert. " + "Starter ikke jobb (Kafka-meldingen committes).",
                                            )
                                        }
                                    }
                                    logger.info("Jobb '${jobbInfo.jobb}' ferdig")
                                } catch (e: Exception) {
                                    logger.error("Jobb '${jobbInfo.jobb}' feilet", e)
                                }
                            }
                        }
                        consumer.commitSync()
                    }
                } catch (e: ManglerJobbParameterException) {
                    logger.warn("Mangler parameter årstallOgKvartal i jobb, commit og ignorer meldingen", e)
                    consumer.commitSync()
                } catch (e: WakeupException) {
                    logger.info("Jobblytter is shutting down")
                } catch (e: RetriableException) {
                    logger.error("Kafka consumer got retriable exception", e)
                } catch (e: Exception) {
                    logger.error("Exception is shutting down kafka listener for ${topic.navnMedNamespace}", e)
                    throw e
                }
            }
        }
    }

    private fun cancel() =
        runBlocking {
            logger.info("Stopping kafka consumer job for ${topic.navn}")
            kafkaConsumer.wakeup()
            job.cancelAndJoin()
            logger.info("Stopped kafka consumer job for ${topic.navn}")
        }
}

@Serializable
data class SerializableJobbInfo(
    override val jobb: Jobb,
    override val tidspunkt: String,
    override val applikasjon: String,
    override val parameter: String?,
) : JobbInfo

fun SerializableJobbInfo.tilÅrstallOgKvartal(): ÅrstallOgKvartal? {
    if (this.parameter.isNullOrBlank()) return null
    return try {
        val kvartalDel = this.parameter.split(":").first()
        val deler = kvartalDel.split("-")
        val årstall = deler.first().toInt()
        val kvartal = deler.last().toInt()
        ÅrstallOgKvartal(
            årstall = årstall,
            kvartal = kvartal,
        )
    } catch (e: Exception) {
        throw ManglerJobbParameterException("Kunne ikke parse årstall og kvartal fra parameter: '$parameter'")
    }
}

// Parameter kan være: '<empty>', 'DRY_RUN', '<årstall>-<kvartal>' eller '<årstall>-<kvartal>:DRY_RUN'
fun SerializableJobbInfo.tilDryRun(): Boolean {
    if (this.parameter.isNullOrBlank()) return true // default er dry-run når parameter er tom
    if (this.parameter.startsWith("DRY_RUN")) return true

    val deler = this.parameter.split(":")
    return deler.size == 2 && deler[1].equals("DRY_RUN", ignoreCase = true)
}
