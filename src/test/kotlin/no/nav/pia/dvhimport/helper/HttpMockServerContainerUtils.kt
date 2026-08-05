package no.nav.pia.dvhimport.helper

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonIgnoreUnknownKeys
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import software.xdev.mockserver.client.MockServerClient
import software.xdev.mockserver.model.Format
import software.xdev.mockserver.model.RequestDefinition

class HttpMockServerContainerUtils {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(this::class.java)

        fun createMockServerClient(
            container: GenericContainer<*>,
            port: Int,
        ): MockServerClient {
            logger.info(
                "Oppretter MockServerClient for container '${container.containerName}' med " +
                    "host '${container.host}' og port '${
                        container.getMappedPort(port)
                    }'",
            )
            return MockServerClient(
                container.host,
                container.getMappedPort(port),
            )
        }

        fun resetAllExpectations(client: MockServerClient) {
            val allExpectations = hentAlleExpectationIds(client = client)

            runBlocking {
                allExpectations.forEach { expectation ->
                    client.clear(expectation.id)
                }
                logger.info("Funnet og slettet '${allExpectations.size}' aktive expectations")
            }
            client.reset()
        }

        private fun hentAlleExpectationIds(client: MockServerClient): List<Expectation> =
            runBlocking {
                val alleAktiveRequestDefinition: RequestDefinition? = null
                val activeExpectations = client.retrieveActiveExpectations(alleAktiveRequestDefinition, Format.JSON)
                Json.decodeFromString<List<Expectation>>(activeExpectations)
            }
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Serializable
    @JsonIgnoreUnknownKeys
    internal data class Expectation(
        val id: String,
    )
}
