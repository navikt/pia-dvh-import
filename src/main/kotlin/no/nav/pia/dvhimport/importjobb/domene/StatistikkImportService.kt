package no.nav.pia.dvhimport.importjobb.domene

import com.google.cloud.storage.Storage
import no.nav.pia.dvhimport.storage.BucketKlient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.RoundingMode


class StatistikkImportService(
    private val bucketName: String,
    private val fileName: String,
    gcpStorage: Storage
) {

    private val bucketKlient = BucketKlient(gcpStorage = gcpStorage, bucketName = bucketName)
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    fun start() {
        logger.info("Starter import av kvartalsvisstatistikk for sykefravær")
        try {
            val statistikk: List<SykefraværsstatistikkDto> = getStatistikk(fileName)
            logger.info("Antall rader med statistikk i bucket '$bucketName' med filnavn '$fileName': ${statistikk.size}")
            val sykefraværsprosentLand = beregnSykefraværsprosentForLand(statistikk)
            logger.info("Sykefraværsprosent for land er: '$sykefraværsprosentLand'")
        } catch (e: Exception) {
            logger.warn("Fikk exception med melding '${e.message}'", e)
        }
    }

    private fun getStatistikk(filnavn: String): List<SykefraværsstatistikkDto> =
        bucketKlient.getFromFile(filnavn = filnavn)


    companion object {
        fun beregnSykefraværsprosentForLand(statistikk: List<SykefraværsstatistikkDto>): BigDecimal {
            val sumAntallTapteDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.tapteDagsverk }
            val sumAntallMuligeDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.muligeDagsverk }
            val sykefraværsprosentLand =
                StatistikkUtils.kalkulerSykefraværsprosent(sumAntallTapteDagsverk, sumAntallMuligeDagsverk)
            return sykefraværsprosentLand.setScale(1, RoundingMode.HALF_UP)
        }
    }
}