package no.nav.pia.dvhimport

import com.google.cloud.NoCredentials
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
import no.nav.pia.dvhimport.storage.BucketKlient


fun main() {
    val naisEnvironment = NaisEnvironment()
    val storageURL = naisEnvironment.googleCloudStorageUrl
    val brukÅrOgKvartalIPathTilFilene: Boolean
    val storage: Storage


    if (storageURL.startsWith("https://")) {
        brukÅrOgKvartalIPathTilFilene = true
        storage = StorageOptions.getDefaultInstance().service // Https / Credentials i GCP (workload identity federation)
    } else {
        brukÅrOgKvartalIPathTilFilene = false
        val projectId = "fake-google-cloud-storage-container-project"
        storage = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setHost(storageURL)
            .setProjectId(projectId)
            .build()
            .service // Http / No credentials -> bare for testing med testcontainers
    }

    Jobblytter(
        statistikkImportService = StatistikkImportService(
            bucketKlient = BucketKlient(gcpStorage = storage, bucketName = naisEnvironment.statistikkBucketName),
            brukÅrOgKvartalIPathTilFilene = brukÅrOgKvartalIPathTilFilene
        )
    ).run()
    embeddedServer(Netty, port = 8080, host = "0.0.0.0", module = Application::dvhImport).start(wait = true)
}

fun Application.dvhImport() {
    configureMonitoring()
    configureSerialization()
    configureRouting()
}
