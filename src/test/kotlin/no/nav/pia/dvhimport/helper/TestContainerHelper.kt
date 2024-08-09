package no.nav.pia.dvhimport.helper

import io.kotest.matchers.string.shouldContain
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.json
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import org.testcontainers.images.builder.ImageFromDockerfile
import java.time.Duration
import kotlin.io.path.Path

class TestContainerHelper {
    companion object {
        val log: Logger = LoggerFactory.getLogger(TestContainerHelper::class.java)
        val network = Network.newNetwork()
        val kafka = KafkaContainerHelper(network = network, log = log)
        val googleCloudStorage = GoogleCloudStorageContainerHelper(network = network, log = log)

        val dvhImportApplikasjon =
            GenericContainer(
                ImageFromDockerfile().withDockerfile(Path("./Dockerfile"))
            )
                .withNetwork(network)
                .withExposedPorts(8080)
                .withLogConsumer(Slf4jLogConsumer(log).withPrefix("dvhImport").withSeparateOutputStreams())
                .withEnv(
                    kafka.envVars() +
                            mapOf(
                                "NAIS_CLUSTER_NAME" to "lokal",
                                )
                                .plus(googleCloudStorage.envVars())
                )
                .dependsOn(kafka.kafkaContainer)
                .dependsOn(googleCloudStorage.container)
                .waitingFor(HttpWaitStrategy().forPath("/internal/isalive").withStartupTimeout(Duration.ofSeconds(20)))
                .apply {
                    start()
                }

        infix fun GenericContainer<*>.shouldContainLog(regex: Regex) = logs shouldContain regex
    }
}

private val httpClient = HttpClient(CIO) {
    install(ContentNegotiation) {
        json()
    }
}

private suspend fun GenericContainer<*>.performRequest(
    url: String,
    config: HttpRequestBuilder.() -> Unit = {},
) =
    httpClient.request {
        config()
        header(HttpHeaders.Accept, "application/json")
        url {
            protocol = URLProtocol.HTTP
            host = this@performRequest.host
            port = firstMappedPort
            path(url)
        }
    }

internal suspend fun GenericContainer<*>.performGet(url: String, config: HttpRequestBuilder.() -> Unit = {}) =
    performRequest(url) {
        config()
        method = HttpMethod.Get
    }
