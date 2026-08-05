package no.nav.pia.dvhimport.helper

import io.kotest.matchers.collections.shouldContainAll
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.helper.HttpMockServerContainerUtils.Companion.createMockServerClient
import no.nav.pia.dvhimport.helper.HttpMockServerContainerUtils.Companion.resetAllExpectations
import no.nav.pia.dvhimport.varsling.SlackVarsler.SlackMelding
import org.slf4j.Logger
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import software.xdev.mockserver.client.MockServerClient
import software.xdev.mockserver.model.HttpRequest
import software.xdev.testcontainers.mockserver.containers.MockServerContainer
import software.xdev.testcontainers.mockserver.containers.MockServerContainer.PORT

class SlackWebhookContainerHelper(
    network: Network = Network.newNetwork(),
    private val log: Logger,
) {
    private val networkAlias = "slack-webhook-container"
    private val port =
        PORT // mockserver default port er 1080 som MockServerContainer() eksponerer selv med "this.addExposedPort(1080);"
    private var mockServerClient: MockServerClient? = null

    private val dockerImageName = DockerImageName.parse("xdevsoftware/mockserver:2.50.9")
    val container: GenericContainer<*> = MockServerContainer(dockerImageName)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withExposedPorts(port)
        .withLogConsumer(Slf4jLogConsumer(log).withPrefix(networkAlias).withSeparateOutputStreams())
        .withEnv(
            mapOf(
                "MOCKSERVER_LIVENESS_HTTP_GET_PATH" to "/isRunning",
                "SERVER_PORT" to "$port",
                "TZ" to "Europe/Oslo",
            ),
        )
        .waitingFor(Wait.forHttp("/isRunning").forStatusCode(200))
        .apply {
            start()
        }.also {
            mockServerClient = createMockServerClient(container = it, port = port)
            log.info("Startet (mock) Slack webhook container for network '$network' og port '$port'")
        }

    fun envVars() =
        mapOf(
            "SLACK_WEBHOOK_URL" to "http://$networkAlias:$port",
        )

    internal fun slettAlleMeldinger() = resetAllExpectations(client = mockServerClient!!)

    internal fun shouldHaveReceived(meldinger: List<SlackMelding>) {
        val filter = HttpRequest.request()
            .withMethod("POST")
            .withPath("/")
        val mottatteBodies = mockServerClient!!
            .retrieveRecordedRequests(filter)
            .map { it.bodyAsString }
        meldinger.map { Json.encodeToString(value = it) } shouldContainAll mottatteBodies
    }
}
