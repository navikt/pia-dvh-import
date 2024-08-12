package no.nav.pia.dvhimport.importjobb.domene

import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkMelding
import no.nav.pia.dvhimport.storage.BucketKlient
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.RoundingMode


class StatistikkImportService(
    private val bucketKlient: BucketKlient,
    private val brukKvartalIPath: Boolean
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val eksportProdusent by lazy {
        EksportProdusent(kafkaConfig = KafkaConfig())
    }

    fun importAlleKategorier() {
        logger.info("Starter import av sykefraværsstatistikk for alle statistikkkategorier")
        val kvartal = "2024K1" // TODO: hent kvartal som skal importeres

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for alle kategorier")
            return
        }
        import<LandSykefraværsstatistikkDto>(Statistikkategori.LAND, kvartal)
        import<SektorSykefraværsstatistikkDto>(Statistikkategori.SEKTOR, kvartal)
        import<NæringSykefraværsstatistikkDto>(Statistikkategori.NÆRING, kvartal)
        import<NæringskodeSykefraværsstatistikkDto>(Statistikkategori.NÆRINGSKODE, kvartal)
        import<VirksomhetSykefraværsstatistikkDto>(Statistikkategori.VIRKSOMHET, kvartal)
        importViksomhetMetadata()
    }

    fun importForKategori(kategori: Statistikkategori) {
        logger.info("Starter import av sykefraværsstatistikk for kategori '${kategori}'")

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for kategori '${kategori}'")
            return
        }

        val kvartal = "2024K1" // TODO: hent kvartal som skal importeres

        when (kategori) {
            Statistikkategori.LAND -> {
                val statistikk = import<LandSykefraværsstatistikkDto>(Statistikkategori.LAND, kvartal)
                sendTilKafka(kvartal = kvartal, statistikkategori = Statistikkategori.LAND, statistikk = statistikk)
            }
            Statistikkategori.SEKTOR -> {
                val statistikk = import<SektorSykefraværsstatistikkDto>(Statistikkategori.SEKTOR, kvartal)
                sendTilKafka(kvartal = kvartal, statistikkategori = Statistikkategori.SEKTOR, statistikk = statistikk)
            }
            Statistikkategori.NÆRING -> {
                val statistikk = import<NæringSykefraværsstatistikkDto>(Statistikkategori.NÆRING, kvartal)
                sendTilKafka(kvartal = kvartal, statistikkategori = Statistikkategori.NÆRING, statistikk = statistikk)
            }
            Statistikkategori.NÆRINGSKODE -> {
                import<NæringskodeSykefraværsstatistikkDto>(Statistikkategori.NÆRINGSKODE, kvartal)
            }
            Statistikkategori.VIRKSOMHET -> {
                import<VirksomhetSykefraværsstatistikkDto>(Statistikkategori.VIRKSOMHET, kvartal)
            }
            Statistikkategori.VIRKSOMHET_METADATA -> {
                importViksomhetMetadata()
            }
        }
    }


    private fun importViksomhetMetadata(){
        logger.info("Starter import av virksomhet metadata")

        val kvartal = "2024K1" // TODO: hent kvartal som skal importeres
        val path = if (brukKvartalIPath) kvartal else ""
        bucketKlient.ensureFileExists(path, Statistikkategori.VIRKSOMHET_METADATA.tilFilnavn())

        try {
            val statistikk = hentStatistikk(
                kvartal = kvartal,
                kategori = Statistikkategori.VIRKSOMHET_METADATA,
                brukKvartalIPath = brukKvartalIPath
            )
            val virksomhetMetadataDtoList: List<VirksomhetMetadataDto> =
                statistikk.toVirksomhetMetadataDto()
            // kontroll
            logger.info("Importert metadata for '${virksomhetMetadataDtoList.size}' virksomhet-er")
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
        }

    }

    private inline fun <reified T: Sykefraværsstatistikk> import(kategori: Statistikkategori, kvartal: String): List<T> {
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
            return sykefraværsstatistikkDtoList
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
            return emptyList()
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
            if (dvhStatistikk.isNullOrEmpty())
                return emptyList()

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

    fun sendTilKafka(
        kvartal: String,
        statistikkategori: Statistikkategori,
        statistikk: List<SykefraværsstatistikkDto>
    ) {
        statistikk.forEach {
            eksportProdusent.sendMelding(
                melding = SykefraværsstatistikkMelding(
                    kvartal = kvartal,
                    statistikkategori = statistikkategori,
                    sykefraværsstatistikk = it
                )
            )
        }
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