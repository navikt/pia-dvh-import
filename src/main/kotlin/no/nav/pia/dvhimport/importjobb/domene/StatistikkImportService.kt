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
    gcpStorage: Storage,
) {
    private val kvartal: String = "2024K1" // TODO: hent kvartal som skal importeres
    private val bucketKlient = BucketKlient(gcpStorage = gcpStorage, bucketName = bucketName)
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    fun start() {
        logger.info("Starter import av sykefraværsstatistikk for alle statistikkkategorier")
        try {

            val landStatistikk: List<LandSykefraværsstatistikkDto> =
                hentStatistikk(kvartal = kvartal, kategori = Statistikkategori.LAND).tilLandSykefraværsstatistikkDto()
            val virksomhetStatistikk: List<VirksomhetSykefraværsstatistikkDto> =
                hentStatistikk(
                    kvartal = kvartal,
                    kategori = Statistikkategori.VIRKSOMHET
                ).tilVirksomhetSykefraværsstatistikkDto()
            // kontroll tall
            val sykefraværsprosentLand = beregnSykefraværsprosentForLand(virksomhetStatistikk)
            logger.info("Sykefraværsprosent beregnet for land er: '$sykefraværsprosentLand', sykefraværsprosent for land er: '{${landStatistikk.first().prosent}}'")
        } catch (e: Exception) {
            logger.warn("Fikk exception med melding '${e.message}'", e)
        }
    }


    private fun hentStatistikk(kvartal: String, kategori: Statistikkategori): List<String> {
        try {
            val fileName = kategori.filnavn()
            val dvhStatistikk = bucketKlient.getFromFile(path = kvartal, fileName = fileName)
            val statistikk: List<String> =
                dvhStatistikk.tilGeneriskStatistikk()
            logger.info("Antall rader med statistikk i bucket '$bucketName', i mappe '$kvartal' med filnavn '$fileName': ${statistikk.size}")
            return statistikk
        } catch (e: Exception) {
            logger.warn("Fikk exception med melding '${e.message}'", e)
            return emptyList()
        }
    }


    companion object {
        private val logger: Logger = LoggerFactory.getLogger(this::class.java)

        private fun Statistikkategori.filnavn(): String =
            when (this) {
                Statistikkategori.LAND -> "land.json"
                Statistikkategori.SEKTOR -> "sektor.json"
                Statistikkategori.NÆRING -> "naering.json"
                Statistikkategori.NÆRINGSKODE -> "naeringskode.json"
                Statistikkategori.VIRKSOMHET -> "virksomhet.json"
                Statistikkategori.VIRKSOMHET_METADATA -> "virksomhet_metadata.json"
            }

        private fun sanityzeOrgnr(jsonElement: String): String =
            jsonElement.replace("[0-9]{9}".toRegex(), "*********")

        private fun String.tilLandSykefraværsstatistikkDto(): LandSykefraværsstatistikkDto =
            Json.decodeFromString<LandSykefraværsstatistikkDto>(this)

        fun List<String>.tilLandSykefraværsstatistikkDto(): List<LandSykefraværsstatistikkDto> =
            this.map { statistikkJson ->
                kotlin.runCatching {
                    statistikkJson.tilLandSykefraværsstatistikkDto()
                }.onFailure { failure ->
                    logger.warn("Kunne ikke deserialize følgende element: '${sanityzeOrgnr(statistikkJson)}' til LandSykefraværsstatistikkDto. Fikk følgende melding: '$failure'")
                }
            }.mapNotNull { it.getOrNull() }

        private fun String.tilVirksomhetSykefraværsstatistikkDto(): VirksomhetSykefraværsstatistikkDto =
            Json.decodeFromString<VirksomhetSykefraværsstatistikkDto>(this)

        fun List<String>.tilVirksomhetSykefraværsstatistikkDto(): List<VirksomhetSykefraværsstatistikkDto> =
            this.map { statistikkJson ->
                kotlin.runCatching {
                    statistikkJson.tilVirksomhetSykefraværsstatistikkDto()
                }.onFailure { failure ->
                    logger.warn("Kunne ikke deserialize følgende element: '${sanityzeOrgnr(statistikkJson)}' til VirksomhetSykefraværsstatistikkDto. Fikk følgende melding: '$failure'")
                }
            }.mapNotNull { it.getOrNull() }

        fun String.tilGeneriskStatistikk(): List<String> =
            Json.decodeFromString<JsonArray>(this).map {
                it.toString()
            }.toList()


        fun beregnSykefraværsprosentForLand(statistikk: List<VirksomhetSykefraværsstatistikkDto>): BigDecimal {
            val sumAntallTapteDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.tapteDagsverk }
            val sumAntallMuligeDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.muligeDagsverk }
            val sykefraværsprosentLand =
                StatistikkUtils.kalkulerSykefraværsprosent(sumAntallTapteDagsverk, sumAntallMuligeDagsverk)
            return sykefraværsprosentLand.setScale(1, RoundingMode.HALF_UP)
        }
    }
}