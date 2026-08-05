package no.nav.pia.dvhimport.varsling

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.anyUrl
import com.github.tomakehurst.wiremock.client.WireMock.containing
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class SlackVarslerTest {

    private lateinit var wireMock: WireMockServer

    @BeforeTest
    fun setup() {
        wireMock = WireMockServer(options().dynamicPort())
        wireMock.start()
    }

    @AfterTest
    fun teardown() {
        wireMock.stop()
    }

    @Test
    fun `send poster meldingen som JSON til webhook`() {
        wireMock.stubFor(post(urlEqualTo("/webhook")).willReturn(aResponse().withStatus(200)))

        val varsler = SlackVarsler(webhookUrl = "${wireMock.baseUrl()}/webhook")
        varsler.send("📥 Import startet for 2. kvartal 2026")

        wireMock.verify(
            postRequestedFor(urlEqualTo("/webhook"))
                .withRequestBody(containing("Import startet for 2. kvartal 2026")),
        )
    }

    @Test
    fun `send er no-op når webhookUrl er blank`() {
        val varsler = SlackVarsler(webhookUrl = "")
        varsler.send("noe som ikke skal sendes")

        wireMock.verify(0, anyRequestedFor(anyUrl()))
    }
}
