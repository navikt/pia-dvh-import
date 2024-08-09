package no.nav.pia.dvhimport.helper

import ia.felles.integrasjoner.jobbsender.Jobb
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import kotlinx.coroutines.time.withTimeoutOrNull
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*


class KafkaContainerHelper(
    network: Network = Network.newNetwork(),
    log: Logger = LoggerFactory.getLogger(KafkaContainerHelper::class.java),
) {
    private val kafkaNetworkAlias = "kafkaContainer"
    private var adminClient: AdminClient
    private var kafkaProducer: KafkaProducer<String, String>

    val kafkaContainer = KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.4.3")
    )
        .withKraft()
        .withNetwork(network)
        .withNetworkAliases(kafkaNetworkAlias)
        .withLogConsumer(Slf4jLogConsumer(log).withPrefix(kafkaNetworkAlias).withSeparateOutputStreams())
        .withEnv(
            mapOf(
                "KAFKA_LOG4J_LOGGERS" to "org.apache.kafka.image.loader.MetadataLoader=WARN",
                "KAFKA_AUTO_LEADER_REBALANCE_ENABLE" to "false",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS" to "1",
                "TZ" to TimeZone.getDefault().id
            )
        )
        .withCreateContainerCmdModifier { cmd -> cmd.withName("$kafkaNetworkAlias-${System.currentTimeMillis()}") }
        .waitingFor(HostPortWaitStrategy())
        .apply {
            start()
            adminClient = AdminClient.create(mapOf(BOOTSTRAP_SERVERS_CONFIG to this.bootstrapServers))
            createTopics()
            kafkaProducer = producer()
        }

    fun nyKonsument(topic: KafkaTopics) =
        KafkaConfig(
            brokers = kafkaContainer.bootstrapServers,
            truststoreLocation = "",
            keystoreLocation = "",
            credstorePassword = "",
        )
            .consumerProperties(konsumentGruppe = topic.konsumentGruppe)
            .let { config ->
                KafkaConsumer(config, StringDeserializer(), StringDeserializer())
            }

    suspend fun ventOgKonsumerKafkaMeldinger(
        key: String,
        konsument: KafkaConsumer<String, String>,
        block: (meldinger: List<String>) -> Unit,
    ) {
        withTimeout(Duration.ofSeconds(5)) {
            launch {
                while (this.isActive) {
                    val records = konsument.poll(Duration.ofMillis(50))

                    println("[DEBUG] -------> got records: $records ")
                    records.forEach {
                        println("[DEBUG] -------> record is: $it ")
                        println("[DEBUG] -------> record key is: ${it.key()} ")
                    }

                    val meldinger = records
                        .filter { it.key() == key }
                        .map { it.value() }
                    if (meldinger.isNotEmpty()) {
                        block(meldinger)
                        break
                    }
                }
            }
        }
    }

    fun envVars() = mapOf(
        "KAFKA_BROKERS" to "BROKER://$kafkaNetworkAlias:9092,PLAINTEXT://$kafkaNetworkAlias:9092",
        "KAFKA_TRUSTSTORE_PATH" to "",
        "KAFKA_KEYSTORE_PATH" to "",
        "KAFKA_CREDSTORE_PASSWORD" to "",
    )

    private fun createTopics() {
        adminClient.createTopics(
            listOf(
                NewTopic(KafkaTopics.PIA_JOBBLYTTER.navn, 1, 1.toShort()),
                NewTopic(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER.navn, 1, 1.toShort()),
            )
        )
    }

    private fun KafkaContainer.producer(): KafkaProducer<String, String> =
        KafkaProducer(
            mapOf(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to this.bootstrapServers,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "PLAINTEXT",
                ProducerConfig.ACKS_CONFIG to "1",
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to "5",
                ProducerConfig.LINGER_MS_CONFIG to "0",
                ProducerConfig.RETRIES_CONFIG to "0",
                ProducerConfig.BATCH_SIZE_CONFIG to "0",
                SaslConfigs.SASL_MECHANISM to "PLAIN"
            ),
            StringSerializer(),
            StringSerializer()
        )

    fun sendOgVentTilKonsumert(nøkkel: String, melding: String, topic: KafkaTopics) {
        runBlocking {
            val sendtMelding = kafkaProducer.send(ProducerRecord(topic.navnMedNamespace, nøkkel, melding)).get()
            ventTilKonsumert(
                konsumentGruppeId = topic.konsumentGruppe,
                recordMetadata = sendtMelding
            )
        }
    }


    fun sendJobbMelding(jobb: Jobb) {
        sendOgVentTilKonsumert(
            nøkkel = jobb.name,
            melding = """{
                "jobb": "${jobb.name}",
                "tidspunkt": "2023-01-01T00:00:00.000Z",
                "applikasjon": "pia-dvh-import"
            }""".trimIndent(),
            topic = KafkaTopics.PIA_JOBBLYTTER,
        )
    }

    private suspend fun ventTilKonsumert(
        konsumentGruppeId: String,
        recordMetadata: RecordMetadata
    ) =
        withTimeoutOrNull(Duration.ofSeconds(5)) {
            do {
                delay(timeMillis = 1L)
            } while (consumerSinOffset(
                    consumerGroup = konsumentGruppeId,
                    topic = recordMetadata.topic()
                ) <= recordMetadata.offset()
            )
        }


    private fun consumerSinOffset(consumerGroup: String, topic: String): Long {
        val offsetMetadata = adminClient.listConsumerGroupOffsets(consumerGroup)
            .partitionsToOffsetAndMetadata().get()
        return offsetMetadata[offsetMetadata.keys.firstOrNull { it.topic().contains(topic) }]?.offset() ?: -1
    }

}
