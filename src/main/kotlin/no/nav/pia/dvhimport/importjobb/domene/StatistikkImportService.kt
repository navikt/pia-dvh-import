package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.datetime.Clock
import kotlinx.datetime.toLocalDateTime
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.antallDagerTilPubliseringsdato
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.erFørPubliseringsdato
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.sjekkPubliseringErIDag
import no.nav.pia.dvhimport.importjobb.domene.Publiseringsdato.Companion.timeZone
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.PubliseringsdatoMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.VirksomhetMetadataMelding
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import no.nav.pia.dvhimport.storage.BucketKlient
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
                sendTilKafka(
                    kvartal = kvartal,
                    statistikkategori = Statistikkategori.NÆRINGSKODE,
                    statistikk = statistikk
                )
            }

            Statistikkategori.VIRKSOMHET -> {
                val statistikk = import<VirksomhetSykefraværsstatistikkDto>(Statistikkategori.VIRKSOMHET, path)
                sendTilKafka(
                    kvartal = kvartal,
                    statistikkategori = Statistikkategori.VIRKSOMHET,
                    statistikk = statistikk
                )
            }

            Statistikkategori.VIRKSOMHET_METADATA -> {
                val metadata = importViksomhetMetadata()
                sendMetadataTilKafka(kvartal = kvartal, metadata = metadata)
            }
        }
    }

    fun importOgEksportPubliseringsdato() {
        val iDag = Clock.System.now().toLocalDateTime(timeZone)
        val publiseringsdatoer = importPubliseringsdato()

        val publiseringsDatoErIDag = sjekkPubliseringErIDag(publiseringsdatoer, iDag)
        if (publiseringsDatoErIDag != null) {
            logger.info(
                "Publiseringsdato er i dag ${publiseringsDatoErIDag.offentligDato}, " +
                        "og kvartal som skal importeres er: " +
                        "${publiseringsDatoErIDag.tilPubliseringsdato().årstall}/${publiseringsDatoErIDag.tilPubliseringsdato().kvartal}"
            )
        }

        val nestePubliseringsdato = nestePubliseringsdato(
            publiseringsdatoer,
            iDag
        )

        logger.info(
            "Neste publiseringsdato er ${nestePubliseringsdato?.dato}, " +
                    "og neste importert kvartal blir ${nestePubliseringsdato?.årstall}/${nestePubliseringsdato?.kvartal}"
        )

        publiseringsdatoer.forEach {
            eksportProdusent.sendMelding(
                melding = PubliseringsdatoMelding(
                    kvartal = "2024",
                    publiseringsdato = it
                )
            )
        }
    }

    fun importPubliseringsdato(): List<PubliseringsdatoDto> {
        logger.info("Starter import av publiseringsdato")
        val år = 2024
        val path = if (brukÅrOgKvartalIPathTilFilene) "$år" else ""

        bucketKlient.ensureFileExists(
            path = path,
            fileName = Metadata.PUBLISERINGSDATO.tilFilnavn()
        )

        return try {
            val publiseringsdatoer = hentInnhold(
                path = path,
                kilde = Metadata.PUBLISERINGSDATO
            )
            logger.info("Antall rader med publiseringsdatoer: ${publiseringsdatoer.size}")
            publiseringsdatoer.tilPubliseringsdatoDto()
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
            emptyList()
        }
    }


    private fun hentÅrstallOgKvartal() =
        Pair("2024", "K1")        // TODO: hent kvartal som skal importeres fra en eller annen tjeneste

    private fun importViksomhetMetadata(): List<VirksomhetMetadataDto> {
        logger.info("Starter import av virksomhet metadata")
        val kvartal = hentÅrstallOgKvartal()
        val path = if (brukÅrOgKvartalIPathTilFilene) "${kvartal.first}/${kvartal.second}" else ""

        bucketKlient.ensureFileExists(
            path = path,
            fileName = Statistikkategori.VIRKSOMHET_METADATA.tilFilnavn()
        )
        return try {
            val statistikk = hentInnhold(
                path = path,
                kilde = Statistikkategori.VIRKSOMHET_METADATA,
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
        kategori: Statistikkategori,
        path: String
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

    private fun hentInnhold(path: String, kilde: DvhDatakilde): List<String> {
        val result: List<String> = try {
            val fileName = kilde.tilFilnavn()
            val innhold = bucketKlient.getFromFile(
                path = path,
                fileName = fileName
            )
            if (innhold.isNullOrEmpty())
                return emptyList()

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

    private fun sendMetadataTilKafka(
        kvartal: String,
        metadata: List<VirksomhetMetadataDto>
    ) {
        metadata.forEach {
            eksportProdusent.sendMelding(
                melding = VirksomhetMetadataMelding(
                    kvartal = kvartal,
                    virksomhetMetadata = it
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

        fun nestePubliseringsdato(
            publiseringsdatoer: List<PubliseringsdatoDto>,
            fraDato: kotlinx.datetime.LocalDateTime
        ): NestePubliseringsdato? {
            val nestPubliseringsdato = publiseringsdatoer.map { it.tilPubliseringsdato() }
                .filter { fraDato.erFørPubliseringsdato(it) }
                .sortedWith(compareBy { fraDato.antallDagerTilPubliseringsdato(it) }).firstOrNull()

            if (nestPubliseringsdato != null) {
                return NestePubliseringsdato(
                    årstall = nestPubliseringsdato.årstall,
                    kvartal = nestPubliseringsdato.kvartal,
                    dato = nestPubliseringsdato.offentligDato
                )
            } else {
                return null
            }
        }


        private fun sanityzeOrgnr(jsonElement: String): String =
            jsonElement.replace("[0-9]{9}".toRegex(), "*********")
    }
}