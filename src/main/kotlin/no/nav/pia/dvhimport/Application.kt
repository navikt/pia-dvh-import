package no.nav.pia.dvhimport

import NaisEnvironment
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import no.nav.pia.dvhimport.importjobb.domene.StatistikkImportService
import no.nav.pia.dvhimport.importjobb.kafka.Jobblytter
import no.nav.pia.dvhimport.konfigurasjon.plugins.configureMonitoring
import no.nav.pia.dvhimport.konfigurasjon.plugins.configureRouting
import no.nav.pia.dvhimport.konfigurasjon.plugins.configureSerialization


fun main() {
    val storage: Storage = StorageOptions.getDefaultInstance().service // Http
    val naisEnvironment = NaisEnvironment()

    Jobblytter(
        statistikkImportService = StatistikkImportService(
            gcpStorage = storage,
            bucketName = naisEnvironment.statistikkBucketName
        )
    ).run()
    embeddedServer(Netty, port = 8080, host = "0.0.0.0", module = Application::dvhImport).start(wait = true)
}

fun Application.dvhImport() {
    configureMonitoring()
    configureSerialization()
    configureRouting()
}
