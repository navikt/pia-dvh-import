package no.nav.pia.dvhimport.importjobb.domene

import com.google.cloud.storage.Storage
import no.nav.pia.dvhimport.storage.BucketKlient
import org.slf4j.Logger
import org.slf4j.LoggerFactory


class StatistikkImportService(
    private val bucketName: String,
    private val fileName: String,
    gcpStorage: Storage
) {

    private val bucketKlient = BucketKlient(gcpStorage = gcpStorage, bucketName = bucketName)
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    fun start() {
        logger.info("Starter import av kvartalsvisstatistikk for sykefravær")
        val statistikk: List<SykefraværsstatistikkDto> = getStatistikk(fileName)
        logger.info("Antall rader med statistikk i bucket '$bucketName' med filnavn '$fileName': ${statistikk.size}")
    }


    private fun getStatistikk(filnavn: String): List<SykefraværsstatistikkDto> =
        bucketKlient.getFromFile(filnavn = filnavn)
}