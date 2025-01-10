package no.nav.pia.dvhimport.importjobb

import kotlinx.datetime.Clock
import kotlinx.datetime.toLocalDateTime
import no.nav.pia.dvhimport.importjobb.domene.DvhDatakilde
import no.nav.pia.dvhimport.importjobb.domene.DvhMetadata
import no.nav.pia.dvhimport.importjobb.domene.DvhStatistikkKategori
import no.nav.pia.dvhimport.importjobb.domene.LandSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.NestePubliseringsdato
import no.nav.pia.dvhimport.importjobb.domene.NæringSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.NæringskodeSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.antallDagerTilPubliseringsdato
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.erFørPubliseringsdato
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.sjekkPubliseringErIDag
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.timeZone
import no.nav.pia.dvhimport.importjobb.domene.PubliseringsdatoDto
import no.nav.pia.dvhimport.importjobb.domene.SektorSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.StatistikkUtils
import no.nav.pia.dvhimport.importjobb.domene.Sykefraværsstatistikk
import no.nav.pia.dvhimport.importjobb.domene.SykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetMetadataDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.tilListe
import no.nav.pia.dvhimport.importjobb.domene.tilPubliseringsdato
import no.nav.pia.dvhimport.importjobb.domene.tilPubliseringsdatoDto
import no.nav.pia.dvhimport.importjobb.domene.tilVirksomhetMetadataDto
import no.nav.pia.dvhimport.importjobb.domene.toSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.PubliseringsdatoMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.VirksomhetMetadataMelding
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import no.nav.pia.dvhimport.storage.BucketKlient
import no.nav.pia.dvhimport.storage.Mappestruktur
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.RoundingMode

class ImportService(
    private val bucketKlient: BucketKlient,
    private val brukÅrOgKvartalIPathTilFilene: Boolean,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val eksportProdusent by lazy {
        EksportProdusent(kafkaConfig = KafkaConfig())
    }

    fun importAlleStatistikkKategorier() {
        logger.info("Starter import av sykefraværsstatistikk for alle statistikkkategorier")
        val mappeStruktur = hentMappestruktur()
        val path = if (brukÅrOgKvartalIPathTilFilene) "${mappeStruktur.publiseringsÅr}/${mappeStruktur.sistePubliserteKvartal}" else ""

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for alle kategorier")
            return
        }
        import<LandSykefraværsstatistikkDto>(DvhStatistikkKategori.LAND, path)
        import<SektorSykefraværsstatistikkDto>(DvhStatistikkKategori.SEKTOR, path)
        import<NæringSykefraværsstatistikkDto>(DvhStatistikkKategori.NÆRING, path)
        import<NæringskodeSykefraværsstatistikkDto>(DvhStatistikkKategori.NÆRINGSKODE, path)
        import<VirksomhetSykefraværsstatistikkDto>(DvhStatistikkKategori.VIRKSOMHET, path)
        importViksomhetMetadata()
    }

    fun importForStatistikkKategori(kategori: DvhStatistikkKategori) {
        logger.info("Starter import av sykefraværsstatistikk for kategori '$kategori'")

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for kategori '$kategori'")
            return
        }

        val mappeStruktur = hentMappestruktur()
        val årstallOgKvartal = mappeStruktur.gjeldendeÅrstallOgKvartal()
        val path = if (brukÅrOgKvartalIPathTilFilene) "${mappeStruktur.publiseringsÅr}/${mappeStruktur.sistePubliserteKvartal}" else ""

        when (kategori) {
            DvhStatistikkKategori.LAND -> {
                val statistikk = import<LandSykefraværsstatistikkDto>(DvhStatistikkKategori.LAND, path)
                sendTilKafka(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    statistikk = statistikk,
                )
            }

            DvhStatistikkKategori.SEKTOR -> {
                val statistikk = import<SektorSykefraværsstatistikkDto>(DvhStatistikkKategori.SEKTOR, path)
                sendTilKafka(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    statistikk = statistikk,
                )
            }

            DvhStatistikkKategori.NÆRING -> {
                val statistikk = import<NæringSykefraværsstatistikkDto>(DvhStatistikkKategori.NÆRING, path)
                sendTilKafka(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    statistikk = statistikk,
                )
            }

            DvhStatistikkKategori.NÆRINGSKODE -> {
                val statistikk = import<NæringskodeSykefraværsstatistikkDto>(DvhStatistikkKategori.NÆRINGSKODE, path)
                sendTilKafka(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    statistikk = statistikk,
                )
            }

            DvhStatistikkKategori.VIRKSOMHET -> {
                val statistikk = import<VirksomhetSykefraværsstatistikkDto>(DvhStatistikkKategori.VIRKSOMHET, path)
                sendTilKafka(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    statistikk = statistikk,
                )
            }
        }
    }

    fun importMetadata(kategori: DvhMetadata) {
        logger.info("Starter import av metadata for kategori '$kategori'")

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for kategori '$kategori'")
            return
        }

        val mappeStruktur = hentMappestruktur()
        val årstallOgKvartal = mappeStruktur.gjeldendeÅrstallOgKvartal()

        when (kategori) {
            DvhMetadata.VIRKSOMHET_METADATA -> {
                val metadata = importViksomhetMetadata()
                sendMetadataTilKafka(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    metadata = metadata,
                )
            }

            DvhMetadata.PUBLISERINGSDATO -> {
                importOgEksportPubliseringsdato()
            }
        }
    }

    private fun importOgEksportPubliseringsdato() {
        val årstallOgKvartal = hentMappestruktur().gjeldendeÅrstallOgKvartal()
        val iDag = Clock.System.now().toLocalDateTime(timeZone)
        val publiseringsdatoer = importPubliseringsdato()

        val publiseringsDatoErIDag = sjekkPubliseringErIDag(publiseringsdatoer, iDag)
        if (publiseringsDatoErIDag != null) {
            logger.info(
                "Publiseringsdato er i dag ${publiseringsDatoErIDag.offentligDato}, " +
                    "og kvartal som skal importeres er: " +
                    "${publiseringsDatoErIDag.tilPubliseringsdato().årstall}/${publiseringsDatoErIDag.tilPubliseringsdato().kvartal}",
            )
        }

        val nestePubliseringsdato = nestePubliseringsdato(
            publiseringsdatoer,
            iDag,
        )

        logger.info(
            "Neste publiseringsdato er ${nestePubliseringsdato?.dato}, " +
                "og neste importert kvartal blir ${nestePubliseringsdato?.årstall}/${nestePubliseringsdato?.kvartal}",
        )

        publiseringsdatoer.forEach {
            eksportProdusent.sendMelding(
                melding = PubliseringsdatoMelding(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    publiseringsdato = it,
                ),
            )
        }
    }

    private fun importPubliseringsdato(): List<PubliseringsdatoDto> {
        logger.info("Starter import av publiseringsdato")
        val år = 2024
        val path = if (brukÅrOgKvartalIPathTilFilene) "$år" else ""

        bucketKlient.ensureFileExists(
            path = path,
            fileName = DvhMetadata.PUBLISERINGSDATO.tilFilnavn(),
        )

        return try {
            val publiseringsdatoer = hentInnhold(
                path = path,
                kilde = DvhMetadata.PUBLISERINGSDATO,
            )
            logger.info("Antall rader med publiseringsdatoer: ${publiseringsdatoer.size}")
            publiseringsdatoer.tilPubliseringsdatoDto()
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
            emptyList()
        }
    }

    private fun importViksomhetMetadata(): List<VirksomhetMetadataDto> {
        logger.info("Starter import av virksomhet metadata")
        val mappestruktur = hentMappestruktur()
        val path = if (brukÅrOgKvartalIPathTilFilene) "${mappestruktur.publiseringsÅr}/${mappestruktur.sistePubliserteKvartal}" else ""

        bucketKlient.ensureFileExists(
            path = path,
            fileName = DvhMetadata.VIRKSOMHET_METADATA.tilFilnavn(),
        )
        return try {
            val statistikk = hentInnhold(
                path = path,
                kilde = DvhMetadata.VIRKSOMHET_METADATA,
            )
            val virksomhetMetadataDtoList: List<VirksomhetMetadataDto> =
                statistikk.tilVirksomhetMetadataDto()
            logger.info("Importert metadata for '${virksomhetMetadataDtoList.size}' virksomhet-er")
            virksomhetMetadataDtoList
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
            emptyList()
        }
    }

    private inline fun <reified T : Sykefraværsstatistikk> import(
        kategori: DvhStatistikkKategori,
        path: String,
    ): List<T> {
        bucketKlient.ensureFileExists(path, kategori.tilFilnavn())

        try {
            val statistikk = hentInnhold(
                path = path,
                kilde = kategori,
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

    private fun hentInnhold(
        path: String,
        kilde: DvhDatakilde,
    ): List<String> {
        val result: List<String> = try {
            val fileName = kilde.tilFilnavn()
            val innhold = bucketKlient.getFromFile(
                path = path,
                fileName = fileName,
            )
            if (innhold.isNullOrEmpty()) {
                return emptyList()
            }

            val data: List<String> =
                innhold.tilListe()
            logger.info("Antall rader med data for kilde '$kilde' og path '$path': ${data.size}")
            data
        } catch (e: Exception) {
            logger.warn("Fikk exception med melding '${e.message}'", e)
            emptyList()
        }
        return result
    }

    private fun sendTilKafka(
        årstall: Int,
        kvartal: Int,
        statistikk: List<SykefraværsstatistikkDto>,
    ) {
        statistikk.forEach {
            eksportProdusent.sendMelding(
                melding = SykefraværsstatistikkMelding(
                    årstall = årstall,
                    kvartal = kvartal,
                    sykefraværsstatistikk = it,
                ),
            )
        }
    }

    private fun sendMetadataTilKafka(
        årstall: Int,
        kvartal: Int,
        metadata: List<VirksomhetMetadataDto>,
    ) {
        metadata.forEach {
            val metadataMelding = VirksomhetMetadataMelding(
                årstall = årstall,
                kvartal = kvartal,
                virksomhetMetadata = it,
            )
            eksportProdusent.sendMelding(
                melding = metadataMelding,
            )
        }
    }

    companion object {
        fun hentMappestruktur() =
            Mappestruktur(
                publiseringsÅr = "2024",
                sistePubliserteKvartal = "K2",
            ) // TODO: les mappestruktur fra GCP bucket

        fun kalkulerSykefraværsprosent(statistikk: List<Sykefraværsstatistikk>): BigDecimal {
            val sumAntallTapteDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.tapteDagsverk }
            val sumAntallMuligeDagsverk = statistikk.sumOf { statistikkDto -> statistikkDto.muligeDagsverk }
            val sykefraværsprosentLand =
                StatistikkUtils.kalkulerSykefraværsprosent(sumAntallTapteDagsverk, sumAntallMuligeDagsverk)
            return sykefraværsprosentLand.setScale(1, RoundingMode.HALF_UP)
        }

        fun nestePubliseringsdato(
            publiseringsdatoer: List<PubliseringsdatoDto>,
            fraDato: kotlinx.datetime.LocalDateTime,
        ): NestePubliseringsdato? {
            val nestPubliseringsdato = publiseringsdatoer.map { it.tilPubliseringsdato() }
                .filter { fraDato.erFørPubliseringsdato(it) }
                .sortedWith(compareBy { fraDato.antallDagerTilPubliseringsdato(it) }).firstOrNull()

            if (nestPubliseringsdato != null) {
                return NestePubliseringsdato(
                    årstall = nestPubliseringsdato.årstall,
                    kvartal = nestPubliseringsdato.kvartal,
                    dato = nestPubliseringsdato.offentligDato,
                )
            } else {
                return null
            }
        }

        private fun sanityzeOrgnr(jsonElement: String): String = jsonElement.replace("[0-9]{9}".toRegex(), "*********")
    }
}
