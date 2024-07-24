package no.nav.pia.dvhimport.importjobb.domene

import com.google.cloud.storage.Storage
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
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
    private val kvartal: String = "2024K1" // TODO: hente kvartal som skal importeres
    private val bucketKlient = BucketKlient(gcpStorage = gcpStorage, bucketName = bucketName)
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    fun start() {
        logger.info("Starter import av kvartalsvisstatistikk for sykefravær")
        try {
            val statistikk: List<SykefraværsstatistikkDto> = getStatistikk(path = kvartal, fileName = fileName)
            logger.info("Antall rader med statistikk i bucket '$bucketName', i mappe '$kvartal' med filnavn '$fileName': ${statistikk.size}")
            val sykefraværsprosentLand = beregnSykefraværsprosentForLand(statistikk)
            logger.info("Sykefraværsprosent for land er: '$sykefraværsprosentLand'")
        } catch (e: Exception) {
            logger.warn("Fikk exception med melding '${e.message}'", e)
        }
    }


    private fun getStatistikk(path: String, fileName: String): List<SykefraværsstatistikkDto> {
        val result: String = bucketKlient.getFromFile(path = path, fileName = fileName)
        return result.toSykefraværsstatistikkDtoList()
    }


    companion object {
        private val logger: Logger = LoggerFactory.getLogger(this::class.java)

        private fun sanityzeOrgnr(jsonElement: String): String =
            jsonElement.replace("[0-9]{9}".toRegex(), "*********")

        private fun String.toSykefraværsstatistikkDto(): SykefraværsstatistikkDto =
            Json.decodeFromString<SykefraværsstatistikkDto>(this)

        private fun List<String>.toSykefraværsstatistikkDto(): List<SykefraværsstatistikkDto> =
            this.map { statistikkJson ->
                kotlin.runCatching {
                    statistikkJson.toSykefraværsstatistikkDto()
                }.onFailure { failure ->
                    logger.warn("Kunne ikke deserialize følgende element: '${sanityzeOrgnr(statistikkJson)}' til SykefraværsstatistikkDto. Fikk følgende melding: '$failure'")
                }
            }.mapNotNull { it.getOrNull() }

        fun String.toSykefraværsstatistikkDtoList(): List<SykefraværsstatistikkDto> =
            Json.decodeFromString<JsonArray>(this).map {
                it.toString()
            }.toSykefraværsstatistikkDto()


        fun beregnSykefraværsprosentForLand(statistikk: List<SykefraværsstatistikkDto>): BigDecimal {
            val sumAntallTapteDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.tapteDagsverk }
            val sumAntallMuligeDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.muligeDagsverk }
            val sykefraværsprosentLand =
                StatistikkUtils.kalkulerSykefraværsprosent(sumAntallTapteDagsverk, sumAntallMuligeDagsverk)
            return sykefraværsprosentLand.setScale(1, RoundingMode.HALF_UP)
        }
    }
}