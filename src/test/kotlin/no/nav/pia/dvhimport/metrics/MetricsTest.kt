package no.nav.pia.dvhimport.metrics

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.performGet

import kotlin.test.Test


class MetricsTest {
    @Test
    fun `skal servere metrikker på metrics endepunkt`() {
        runBlocking {
            val metrikkRespons = dvhImportApplikasjon.performGet("/metrics")
            metrikkRespons.status shouldBe HttpStatusCode.OK
            metrikkRespons.bodyAsText() shouldContain "jvm_threads_daemon_threads"
        }
    }
}