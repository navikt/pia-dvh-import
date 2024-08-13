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
    private val brukÅrOgKvartalIPathTilFilene: Boolean
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val eksportProdusent by lazy {
        EksportProdusent(kafkaConfig = KafkaConfig())
    }

    fun importAlleKategorier() {
        logger.info("Starter import av sykefraværsstatistikk for alle statistikkkategorier")
        val årstallOgKvartal = hentÅrstallOgKvartal()
        val path = if (brukÅrOgKvartalIPathTilFilene) "${årstallOgKvartal.first}/${årstallOgKvartal.second}" else ""


        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for alle kategorier")
            return
        }
        import<LandSykefraværsstatistikkDto>(Statistikkategori.LAND, path)
        import<SektorSykefraværsstatistikkDto>(Statistikkategori.SEKTOR, path)
        import<NæringSykefraværsstatistikkDto>(Statistikkategori.NÆRING, path)
        import<NæringskodeSykefraværsstatistikkDto>(Statistikkategori.NÆRINGSKODE, path)
        import<VirksomhetSykefraværsstatistikkDto>(Statistikkategori.VIRKSOMHET, path)
        importViksomhetMetadata()
    }

    fun importForKategori(kategori: Statistikkategori) {
        logger.info("Starter import av sykefraværsstatistikk for kategori '${kategori}'")

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for kategori '${kategori}'")
            return
        }

        val årstallOgKvartal = hentÅrstallOgKvartal()
        val kvartal = "${årstallOgKvartal.first}${årstallOgKvartal.second}"
        val path = if (brukÅrOgKvartalIPathTilFilene) "${årstallOgKvartal.first}/${årstallOgKvartal.second}" else ""

        when (kategori) {
            Statistikkategori.LAND -> {
                val statistikk = import<LandSykefraværsstatistikkDto>(Statistikkategori.LAND, path)
                sendTilKafka(kvartal = kvartal, statistikkategori = Statistikkategori.LAND, statistikk = statistikk)
            }

            Statistikkategori.SEKTOR -> {
                val statistikk = import<SektorSykefraværsstatistikkDto>(Statistikkategori.SEKTOR, path)
                sendTilKafka(kvartal = kvartal, statistikkategori = Statistikkategori.SEKTOR, statistikk = statistikk)
            }

            Statistikkategori.NÆRING -> {
                val statistikk = import<NæringSykefraværsstatistikkDto>(Statistikkategori.NÆRING, path)
                sendTilKafka(kvartal = kvartal, statistikkategori = Statistikkategori.NÆRING, statistikk = statistikk)
            }

            Statistikkategori.NÆRINGSKODE -> {
                val statistikk = import<NæringskodeSykefraværsstatistikkDto>(Statistikkategori.NÆRINGSKODE, path)
                sendTilKafka(kvartal = kvartal, statistikkategori = Statistikkategori.NÆRINGSKODE, statistikk = statistikk)
            }

            Statistikkategori.VIRKSOMHET -> {
                import<VirksomhetSykefraværsstatistikkDto>(Statistikkategori.VIRKSOMHET, path)
            }

            Statistikkategori.VIRKSOMHET_METADATA -> {
                importViksomhetMetadata()
            }
        }
    }


    private fun hentÅrstallOgKvartal() =
        Pair("2024", "K1")        // TODO: hent kvartal som skal importeres fra en eller annen tjeneste


    private fun importViksomhetMetadata() {
        logger.info("Starter import av virksomhet metadata")
        val kvartal = hentÅrstallOgKvartal()
        val path = if (brukÅrOgKvartalIPathTilFilene) "${kvartal.first}/${kvartal.second}" else ""

        bucketKlient.ensureFileExists(
            path = path,
            fileName = Statistikkategori.VIRKSOMHET_METADATA.tilFilnavn()
        )

        try {
            val statistikk = hentStatistikk(
                path = path,
                kategori = Statistikkategori.VIRKSOMHET_METADATA,
            )
            val virksomhetMetadataDtoList: List<VirksomhetMetadataDto> =
                statistikk.toVirksomhetMetadataDto()
            // kontroll
            logger.info("Importert metadata for '${virksomhetMetadataDtoList.size}' virksomhet-er")
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
        }

    }

    private inline fun <reified T : Sykefraværsstatistikk> import(
        kategori: Statistikkategori,
        path: String
    ): List<T> {
        bucketKlient.ensureFileExists(path, kategori.tilFilnavn())

        try {
            val statistikk = hentStatistikk(
                path = path,
                kategori = kategori,
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

    private fun hentStatistikk(path: String, kategori: Statistikkategori): List<String> {
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
            logger.info("Antall rader med statistikk for kategori '$kategori' og path '$path': ${statistikk.size}")
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