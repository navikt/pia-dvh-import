package no.nav.pia.dvhimport.importjobb.domene

import no.nav.pia.dvhimport.storage.BucketKlient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.RoundingMode


class StatistikkImportService(
    private val bucketKlient: BucketKlient,
    private val brukKvartalIPath: Boolean
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    fun importAlleKategorier() {
        logger.info("Starter import av sykefraværsstatistikk for alle statistikkkategorier")
        val kvartal = "2024K1" // TODO: hent kvartal som skal importeres
        bucketKlient.ensureBucketExists()

        import<LandSykefraværsstatistikkDto>(Statistikkategori.LAND, kvartal)
        import<SektorSykefraværsstatistikkDto>(Statistikkategori.SEKTOR, kvartal)
        import<NæringSykefraværsstatistikkDto>(Statistikkategori.NÆRING, kvartal)
        import<VirksomhetSykefraværsstatistikkDto>(Statistikkategori.VIRKSOMHET, kvartal)
    }

    fun importForKategori(kategori: Statistikkategori) {
        logger.info("Starter import av sykefraværsstatistikk for kategori '${kategori}'")
        bucketKlient.ensureBucketExists()

        val kvartal = "2024K1" // TODO: hent kvartal som skal importeres

        when (kategori) {
            Statistikkategori.LAND -> {
                import<LandSykefraværsstatistikkDto>(Statistikkategori.LAND, kvartal)
            }
            Statistikkategori.SEKTOR -> {
                import<SektorSykefraværsstatistikkDto>(Statistikkategori.SEKTOR, kvartal)
            }
            Statistikkategori.NÆRING -> {
                import<NæringSykefraværsstatistikkDto>(Statistikkategori.NÆRING, kvartal)
            }
            Statistikkategori.VIRKSOMHET -> {
                import<VirksomhetSykefraværsstatistikkDto>(Statistikkategori.VIRKSOMHET, kvartal)
            }
            else -> {
                logger.warn("Fant ikke kategori '${kategori}'")
            }
        }
    }


    private inline fun <reified T: Sykefraværsstatistikk> import(kategori: Statistikkategori, kvartal: String) {
        val path = if (brukKvartalIPath) kvartal else ""
        bucketKlient.ensureFileExists(path, kategori.tilFilnavn())

        try {
            val statistikk = hentStatistikk(
                kvartal = kvartal,
                kategori = kategori,
                brukKvartalIPath = brukKvartalIPath
            )
            val sykefraværsstatistikkDtoList: List<T> =
                statistikk.toSykefraværsstatistikkDto<T>()

            // kontroll
            val sykefraværsprosentForKategori = kalkulerSykefraværsprosent(sykefraværsstatistikkDtoList)
            logger.info("Sykefraværsprosent -snitt- for kategori $kategori er: '$sykefraværsprosentForKategori'")
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
        }
    }

    private fun hentStatistikk(kvartal: String, kategori: Statistikkategori, brukKvartalIPath: Boolean): List<String> {
        val path = if (brukKvartalIPath) kvartal else ""
        val result: List<String> = try {
            val fileName = kategori.tilFilnavn()
            val dvhStatistikk = bucketKlient.getFromFile(
                path = path,
                fileName = fileName
            )
            val statistikk: List<String> =
                dvhStatistikk.tilGeneriskStatistikk()
            logger.info("Antall rader med statistikk for kategori '$kategori' og kvartal '$kvartal': ${statistikk.size}")
            statistikk
        } catch (e: Exception) {
            logger.warn("Fikk exception med melding '${e.message}'", e)
            emptyList()
        }
        return result
    }


    companion object {
        fun kalkulerSykefraværsprosent(statistikk: List<Sykefraværsstatistikk>): BigDecimal {
            val sumAntallTapteDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.tapteDagsverk }
            val sumAntallMuligeDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.muligeDagsverk }
            val sykefraværsprosentLand =
                StatistikkUtils.kalkulerSykefraværsprosent(sumAntallTapteDagsverk, sumAntallMuligeDagsverk)
            return sykefraværsprosentLand.setScale(1, RoundingMode.HALF_UP)
        }

        fun Statistikkategori.tilFilnavn(): String =
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
    }
}