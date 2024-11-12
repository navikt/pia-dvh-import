package no.nav.pia.dvhimport.konfigurasjon

import no.nav.pia.dvhimport.NaisEnvironment.Companion.getEnvVar
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringSerializer

class KafkaConfig(
    val brokers: String = getEnvVar("KAFKA_BROKERS"),
    val truststoreLocation: String = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
    val keystoreLocation: String = getEnvVar("KAFKA_KEYSTORE_PATH"),
    val credstorePassword: String = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
) {
    companion object {
        const val CLIENT_ID: String = "pia-dvh-import"
    }

    private fun securityConfigs() =
        mapOf(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG to "",
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to "JKS",
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG to "PKCS12",
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to truststoreLocation,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to credstorePassword,
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to keystoreLocation,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to credstorePassword,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG to credstorePassword,
        )

    fun consumerProperties(konsumentGruppe: String) =
        baseConsumerProperties(konsumentGruppe).apply {
            if (truststoreLocation.isBlank()) {
                put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
                put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            } else {
                putAll(securityConfigs())
            }
        }

    private fun baseConsumerProperties(konsumentGruppe: String) =
        mapOf(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to brokers,
            ConsumerConfig.GROUP_ID_CONFIG to konsumentGruppe,
            ConsumerConfig.CLIENT_ID_CONFIG to CLIENT_ID,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "1000",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        ).toProperties()

    fun producerProperties(): Map<String, Any> {
        val producerConfigs = mutableMapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true, // Den sikrer rekkefølge
            ProducerConfig.ACKS_CONFIG to "all", // Den sikrer at data ikke mistes
            ProducerConfig.CLIENT_ID_CONFIG to CLIENT_ID,
        )
        // For lokal kjøring
        if (truststoreLocation.isNotEmpty()) {
            producerConfigs.putAll(
                mapOf(
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
                    SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG to "",
                    SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to "JKS",
                    SslConfigs.SSL_KEYSTORE_TYPE_CONFIG to "PKCS12",
                    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to truststoreLocation,
                    SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to credstorePassword,
                    SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to keystoreLocation,
                    SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to credstorePassword,
                    SslConfigs.SSL_KEY_PASSWORD_CONFIG to credstorePassword,
                ),
            )
        }
        return producerConfigs.toMap()
    }
}
