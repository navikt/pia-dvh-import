package no.nav.pia.dvhimport.varsling

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Poster informative meldinger til en Slack-kanal via en Incoming Webhook.
 * URL-en settes per miljø (SLACK_WEBHOOK_URL). Er den blank (test/lokal) er send() en no-op.
 * Feil ved sending svelges og logges — en utilgjengelig Slack skal aldri velte importen.
 */
class SlackVarsler(
    private val webhookUrl: String,
    private val httpClient: HttpClient = HttpClient(CIO),
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    @Serializable
    private data class SlackMelding(val text: String)

    fun send(melding: String) {
        if (webhookUrl.isBlank()) {
            logger.info("SLACK_WEBHOOK_URL ikke satt — hopper over Slack-varsel: $melding")
            return
        }
        try {
            runBlocking {
                httpClient.post(webhookUrl) {
                    contentType(ContentType.Application.Json)
                    setBody(Json.encodeToString(SlackMelding(text = melding)))
                }
            }
        } catch (e: Exception) {
            logger.warn("Kunne ikke sende Slack-varsel: $melding", e)
        }
    }
}
