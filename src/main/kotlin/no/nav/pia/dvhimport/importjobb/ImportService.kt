package no.nav.pia.dvhimport.importjobb

import ia.felles.definisjoner.bransjer.Bransje
import ia.felles.definisjoner.bransjer.BransjeId
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import kotlinx.datetime.toLocalDateTime
import no.nav.pia.dvhimport.importjobb.domene.BransjeSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.DvhMetadata
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
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori
import no.nav.pia.dvhimport.importjobb.domene.StatistikkUtils
import no.nav.pia.dvhimport.importjobb.domene.Sykefraværsstatistikk
import no.nav.pia.dvhimport.importjobb.domene.SykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.TapteDagsverkPerVarighetDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetMetadataDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.tilListe
import no.nav.pia.dvhimport.importjobb.domene.tilPubliseringsdato
import no.nav.pia.dvhimport.importjobb.domene.tilPubliseringsdatoDto
import no.nav.pia.dvhimport.importjobb.domene.tilVirksomhetMetadataDto
import no.nav.pia.dvhimport.importjobb.domene.toSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.PubliseringsdatoMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.VirksomhetMetadataMelding
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import no.nav.pia.dvhimport.storage.BucketKlient
import no.nav.pia.dvhimport.storage.BucketKlient.Companion.prosesserIBiter
import no.nav.pia.dvhimport.storage.Mappestruktur
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import java.math.RoundingMode
import java.util.concurrent.atomic.AtomicReference

class ImportService(
    private val bucketKlient: BucketKlient,
    private val brukÅrOgKvartalIPathTilFilene: Boolean,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val eksportProdusent by lazy {
        EksportProdusent(kafkaConfig = KafkaConfig())
    }

    fun importAlleStatistikkKategorier(årstallOgKvartal: ÅrstallOgKvartal) {
        logger.info("Starter import av sykefraværsstatistikk for alle statistikkkategorier")
        val mappeStruktur = årstallOgKvartal.hentMappestruktur()
        val path =
            if (brukÅrOgKvartalIPathTilFilene) "${mappeStruktur.publiseringsÅr}/${mappeStruktur.sistePubliserteKvartal}" else ""

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for alle kategorier")
            return
        }
        import<LandSykefraværsstatistikkDto>(StatistikkKategori.LAND, path)
        import<SektorSykefraværsstatistikkDto>(StatistikkKategori.SEKTOR, path)
        import<NæringSykefraværsstatistikkDto>(StatistikkKategori.NÆRING, path)
        import<NæringskodeSykefraværsstatistikkDto>(StatistikkKategori.NÆRINGSKODE, path)
        import<VirksomhetSykefraværsstatistikkDto>(StatistikkKategori.VIRKSOMHET, path)
        importViksomhetMetadata(årstallOgKvartal)
    }

    fun importForStatistikkKategori(
        kategori: StatistikkKategori,
        årstallOgKvartal: ÅrstallOgKvartal,
    ) {
        logger.info("Starter import av sykefraværsstatistikk for kategori '$kategori'")

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for kategori '$kategori'")
            return
        }

        val mappeStruktur = årstallOgKvartal.hentMappestruktur()
        val årstallOgKvartal = mappeStruktur.gjeldendeÅrstallOgKvartal()
        val path =
            if (brukÅrOgKvartalIPathTilFilene) "${mappeStruktur.publiseringsÅr}/${mappeStruktur.sistePubliserteKvartal}" else ""

        when (kategori) {
            StatistikkKategori.LAND -> {
                val statistikk = import<LandSykefraværsstatistikkDto>(StatistikkKategori.LAND, path)
                sendTilKafka(
                    årstallOgKvartal = årstallOgKvartal,
                    statistikk = statistikk,
                )
            }

            StatistikkKategori.SEKTOR -> {
                val statistikk = import<SektorSykefraværsstatistikkDto>(StatistikkKategori.SEKTOR, path)
                sendTilKafka(
                    årstallOgKvartal = årstallOgKvartal,
                    statistikk = statistikk,
                )
            }

            StatistikkKategori.NÆRING -> {
                val statistikk = import<NæringSykefraværsstatistikkDto>(StatistikkKategori.NÆRING, path)
                sendTilKafka(
                    årstallOgKvartal = årstallOgKvartal,
                    statistikk = statistikk,
                )
            }

            StatistikkKategori.NÆRINGSKODE -> {
                val statistikk = import<NæringskodeSykefraværsstatistikkDto>(StatistikkKategori.NÆRINGSKODE, path)
                sendTilKafka(
                    årstallOgKvartal = årstallOgKvartal,
                    statistikk = statistikk,
                )
            }

            StatistikkKategori.BRANSJE -> {
                val statistikk = importBransje(path = path, årstallOgKvartal = årstallOgKvartal)
                sendTilKafka(
                    årstallOgKvartal = årstallOgKvartal,
                    statistikk = statistikk,
                )
            }

            StatistikkKategori.VIRKSOMHET -> {
                importVirksomheterOgSendTilKafka(
                    path = path,
                    årstallOgKvartal = årstallOgKvartal,
                )
            }
        }
    }

    fun importVirksomheterOgSendTilKafka(
        path: String,
        årstallOgKvartal: ÅrstallOgKvartal,
    ) {
        val skalSendeTilKafka = brukÅrOgKvartalIPathTilFilene == false // TODO: DELETE ME etter load-test
        val sumAntallTapteDagsverk = AtomicReference(BigDecimal(0))
        val sumAntallMuligeDagsverk = AtomicReference(BigDecimal(0))
        val sumAntallVirksomheter = AtomicReference(0)

        runBlocking {
            val sequence = bucketKlient.getFromHugeFileAsSequence<VirksomhetSykefraværsstatistikkDto>(
                path = path,
                fileName = tilFilNavn(StatistikkKategori.VIRKSOMHET),
            )
            sequence.prosesserIBiter(størrelse = 100) { statistikk ->
                logger.info("Sender ${statistikk.size} statistikk for virksomhet til Kafka")
                if (skalSendeTilKafka) {
                    logger.info("Skal IKKE sende til kafka i load-test")
                    sendTilKafka(
                        årstallOgKvartal = årstallOgKvartal,
                        statistikk,
                    )
                }
                sumAntallVirksomheter.getAndAccumulate(statistikk.size) { x, y -> x + y }
                sumAntallMuligeDagsverk.getAndAccumulate(statistikk.sumOf { it.muligeDagsverk }) { x, y -> x + y }
                sumAntallTapteDagsverk.getAndAccumulate(statistikk.sumOf { it.tapteDagsverk }) { x, y -> x + y }
            }
            val sykefraværsprosentForKategori =
                StatistikkUtils.kalkulerSykefraværsprosent(sumAntallTapteDagsverk.get(), sumAntallMuligeDagsverk.get())
            logger.info("Antall statistikk prosessert for kategori ${StatistikkKategori.VIRKSOMHET.name} er: '$sumAntallVirksomheter'")
            logger.info(
                "Sykefraværsprosent -snitt- for kategori ${StatistikkKategori.VIRKSOMHET.name} er: '$sykefraværsprosentForKategori'",
            )
        }
    }

    fun importMetadata(
        kategori: DvhMetadata,
        årstallOgKvartal: ÅrstallOgKvartal,
    ) {
        logger.info("Starter import av metadata for kategori '$kategori'")

        if (!bucketKlient.sjekkBucketExists()) {
            logger.error("Bucket ikke funnet, avbryter import for kategori '$kategori'")
            return
        }

        when (kategori) {
            DvhMetadata.VIRKSOMHET_METADATA -> {
                val metadata = importViksomhetMetadata(årstallOgKvartal)
                sendMetadataTilKafka(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    metadata = metadata,
                )
            }

            DvhMetadata.PUBLISERINGSDATO -> {
                importOgEksportPubliseringsdato(årstallOgKvartal)
            }
        }
    }

    private fun importOgEksportPubliseringsdato(årstallOgKvartal: ÅrstallOgKvartal) {
        val iDag = Clock.System.now().toLocalDateTime(timeZone)
        val publiseringsdatoer = importPubliseringsdato(årstallOgKvartal)

        val publiseringsDatoErIDag = sjekkPubliseringErIDag(publiseringsdatoer, iDag)
        if (publiseringsDatoErIDag != null) {
            logger.info(
                "Publiseringsdato er i dag ${publiseringsDatoErIDag.offentligDato}, " + "og kvartal som skal importeres er: " +
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

    private fun importPubliseringsdato(årstallOgKvartal: ÅrstallOgKvartal): List<PubliseringsdatoDto> {
        logger.info("Starter import av publiseringsdato")
        val path = if (brukÅrOgKvartalIPathTilFilene) "${årstallOgKvartal.årstall}" else ""

        bucketKlient.ensureFileExists(
            path = path,
            fileName = tilFilNavn(DvhMetadata.PUBLISERINGSDATO),
        )

        return try {
            val publiseringsdatoer = hentInnholdForMetadata(
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

    private fun importViksomhetMetadata(årstallOgKvartal: ÅrstallOgKvartal): List<VirksomhetMetadataDto> {
        logger.info("Starter import av virksomhet metadata")
        val path =
            if (brukÅrOgKvartalIPathTilFilene) "${årstallOgKvartal.årstall}/${årstallOgKvartal.kvartal}" else ""

        bucketKlient.ensureFileExists(
            path = path,
            fileName = tilFilNavn(DvhMetadata.VIRKSOMHET_METADATA),
        )
        return try {
            val statistikk = hentInnholdForMetadata(
                path = path,
                kilde = DvhMetadata.VIRKSOMHET_METADATA,
            )
            val virksomhetMetadataDtoList: List<VirksomhetMetadataDto> = statistikk.tilVirksomhetMetadataDto()
            logger.info("Importert metadata for '${virksomhetMetadataDtoList.size}' virksomhet-er")
            virksomhetMetadataDtoList
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
            emptyList()
        }
    }

    private inline fun <reified T : Sykefraværsstatistikk> import(
        kategori: StatistikkKategori,
        path: String,
    ): List<T> {
        bucketKlient.ensureFileExists(path, tilFilNavn(kategori))

        try {
            val statistikk = hentInnholdForStatistikk(
                path = path,
                kilde = kategori,
            )
            val sykefraværsstatistikkDtoList: List<T> = statistikk.toSykefraværsstatistikkDto<T>()

            // kontroll
            val sykefraværsprosentForKategori = kalkulerSykefraværsprosent(sykefraværsstatistikkDtoList)
            logger.info("Sykefraværsprosent -snitt- for kategori $kategori er: '$sykefraværsprosentForKategori'")
            return sykefraværsstatistikkDtoList
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
            return emptyList()
        }
    }

    private fun importBransje(
        path: String,
        årstallOgKvartal: ÅrstallOgKvartal,
    ): List<BransjeSykefraværsstatistikkDto> {
        bucketKlient.ensureFileExists(path, tilFilNavn(StatistikkKategori.NÆRING))

        try {
            val statistikkNæring = hentInnholdForStatistikk(
                path = path,
                kilde = StatistikkKategori.NÆRING,
            )
            val sykefraværsstatistikkNæringDtoList: List<NæringSykefraværsstatistikkDto> =
                statistikkNæring.toSykefraværsstatistikkDto<NæringSykefraværsstatistikkDto>()
            val statistikkNæringskode = hentInnholdForStatistikk(
                path = path,
                kilde = StatistikkKategori.NÆRINGSKODE,
            )
            val sykefraværsstatistikkNæringskodeDtoList: List<NæringskodeSykefraværsstatistikkDto> =
                statistikkNæringskode.toSykefraværsstatistikkDto<NæringskodeSykefraværsstatistikkDto>()

            val sykefraværsstatistikkDtoList: List<BransjeSykefraværsstatistikkDto?> = Bransje.entries.map { bransje ->
                when (bransje.bransjeId) {
                    is BransjeId.Næring -> sykefraværsstatistikkNæringDtoList.filter { dto ->
                        dto.næring == (bransje.bransjeId as BransjeId.Næring).næring
                    }.firstOrNull()?.let {
                        BransjeSykefraværsstatistikkDto(
                            bransje = bransje.navn,
                            årstall = årstallOgKvartal.årstall,
                            kvartal = årstallOgKvartal.kvartal,
                            prosent = it.prosent,
                            tapteDagsverk = it.tapteDagsverk,
                            muligeDagsverk = it.muligeDagsverk,
                            tapteDagsverkGradert = it.tapteDagsverkGradert,
                            tapteDagsverkPerVarighet = it.tapteDagsverkPerVarighet,
                            antallPersoner = it.antallPersoner,
                        )
                    }

                    is BransjeId.Næringskoder -> sykefraværsstatistikkNæringskodeDtoList.filter { dto ->
                        (bransje.bransjeId as BransjeId.Næringskoder).næringskoder.contains(dto.næringskode)
                    }.utleddBransjeStatistikk(
                        årstall = årstallOgKvartal.årstall,
                        kvartal = årstallOgKvartal.kvartal,
                        bransje = bransje,
                    )
                }
            }

            // kontroll
            val sykefraværsprosentForKategori = kalkulerSykefraværsprosent(sykefraværsstatistikkDtoList)
            logger.info("Sykefraværsprosent -snitt- for kategori ${StatistikkKategori.BRANSJE.name} er: '$sykefraværsprosentForKategori'")
            return sykefraværsstatistikkDtoList.filterNotNull()
        } catch (e: Exception) {
            logger.warn("Fikk exception i import prosess med melding '${e.message}'", e)
            return emptyList()
        }
    }

    private fun hentInnholdForMetadata(
        path: String,
        kilde: DvhMetadata,
    ): List<String> = hentInnhold(path = path, kilde = kilde.name, filnavn = tilFilNavn(kilde))

    private fun hentInnholdForStatistikk(
        path: String,
        kilde: StatistikkKategori,
    ): List<String> = hentInnhold(path = path, kilde = kilde.name, filnavn = tilFilNavn(kilde))

    private fun hentInnhold(
        path: String,
        kilde: String,
        filnavn: String,
    ): List<String> {
        val result: List<String> = try {
            val innhold = bucketKlient.getFromFile(
                path = path,
                fileName = filnavn,
            )
            if (innhold.isNullOrEmpty()) {
                return emptyList()
            }

            val data: List<String> = innhold.tilListe()
            logger.info("Antall rader med data for kilde '$kilde' og path '$path': ${data.size}")
            data
        } catch (e: Exception) {
            logger.warn("Fikk exception med melding '${e.message}'", e)
            emptyList()
        }
        return result
    }

    private fun sendTilKafka(
        årstallOgKvartal: ÅrstallOgKvartal,
        statistikk: List<SykefraværsstatistikkDto>,
    ) {
        statistikk.forEach {
            eksportProdusent.sendMelding(
                melding = SykefraværsstatistikkMelding(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
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
        const val ANTALL_SIFRE_I_UTREGNING = 3
        const val ANTALL_SIFRE_I_RESULTAT = 1

        fun tilFilNavn(metadata: DvhMetadata) =
            when (metadata) {
                DvhMetadata.PUBLISERINGSDATO -> "publiseringsdato.json"
                DvhMetadata.VIRKSOMHET_METADATA -> "virksomhet_metadata.json"
            }

        fun tilFilNavn(kategori: StatistikkKategori) =
            when (kategori) {
                StatistikkKategori.LAND -> "land.json"
                StatistikkKategori.SEKTOR -> "sektor.json"
                StatistikkKategori.NÆRING -> "naering.json"
                StatistikkKategori.NÆRINGSKODE -> "naeringskode.json"
                StatistikkKategori.VIRKSOMHET -> "virksomhet.json"
                else -> throw NoSuchElementException("Ingen fil tilgjengelig for kategori '$kategori'")
            }

        fun ÅrstallOgKvartal.hentMappestruktur() =
            Mappestruktur(
                publiseringsÅr = "$årstall",
                sistePubliserteKvartal = "K$kvartal",
            )

        fun kalkulerSykefraværsprosent(statistikk: List<Sykefraværsstatistikk?>): BigDecimal {
            val sumAntallTapteDagsverk =
                statistikk.sumOf { it?.tapteDagsverk ?: ZERO }
            val sumAntallMuligeDagsverk =
                statistikk.sumOf { it?.muligeDagsverk ?: ZERO }
            val sykefraværsprosentForKategori =
                StatistikkUtils.kalkulerSykefraværsprosent(sumAntallTapteDagsverk, sumAntallMuligeDagsverk)
            return sykefraværsprosentForKategori
        }

        fun nestePubliseringsdato(
            publiseringsdatoer: List<PubliseringsdatoDto>,
            fraDato: kotlinx.datetime.LocalDateTime,
        ): NestePubliseringsdato? {
            val nestPubliseringsdato =
                publiseringsdatoer.map { it.tilPubliseringsdato() }.filter { fraDato.erFørPubliseringsdato(it) }
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

        fun List<NæringskodeSykefraværsstatistikkDto>.utleddBransjeStatistikk(
            årstall: Int,
            kvartal: Int,
            bransje: Bransje,
        ): BransjeSykefraværsstatistikkDto? {
            if (this.isEmpty()) {
                return null
            }

            val tapteDagsverk = this.sumOf { it.tapteDagsverk }
            val muligeDagsverk = this.sumOf { it.muligeDagsverk }
            val tapteDagsverkGradert = this.sumOf { it.tapteDagsverkGradert }
            val antallPersoner = this.sumOf { it.antallPersoner }

            var tapteDagsverkPerVarighet = mutableListOf<TapteDagsverkPerVarighetDto>()
            this.forEach {
                tapteDagsverkPerVarighet = tapteDagsverkPerVarighet.aggreger(it.tapteDagsverkPerVarighet)
            }

            return BransjeSykefraværsstatistikkDto(
                bransje = bransje.navn, // Vi sender melding med Bransje.navn, dvs "Barnehager" og IKKE "BARNEHAGER"
                årstall = årstall,
                kvartal = kvartal,
                prosent = tapteDagsverk.divide(muligeDagsverk, ANTALL_SIFRE_I_UTREGNING, RoundingMode.HALF_UP)
                    .multiply(BigDecimal(100)).setScale(ANTALL_SIFRE_I_RESULTAT, RoundingMode.HALF_UP),
                tapteDagsverk = tapteDagsverk,
                muligeDagsverk = muligeDagsverk,
                tapteDagsverkGradert = tapteDagsverkGradert,
                tapteDagsverkPerVarighet = tapteDagsverkPerVarighet,
                antallPersoner = antallPersoner,
            )
        }

        fun MutableList<TapteDagsverkPerVarighetDto>.aggreger(
            items: List<TapteDagsverkPerVarighetDto>,
        ): MutableList<TapteDagsverkPerVarighetDto> {
            items.forEach { item ->
                this.leggTil(item)
            }
            return this.sortedBy { it.varighet }.toMutableList()
        }

        fun MutableList<TapteDagsverkPerVarighetDto>.leggTil(item: TapteDagsverkPerVarighetDto): List<TapteDagsverkPerVarighetDto> {
            var updated = false
            this.let {
                forEachIndexed { i, value ->
                    if (value.varighet == item.varighet && item.tapteDagsverk != null) {
                        it[i] = TapteDagsverkPerVarighetDto(
                            varighet = item.varighet,
                            tapteDagsverk = value.tapteDagsverk?.plus(item.tapteDagsverk) ?: item.tapteDagsverk,
                        )
                        updated = true
                    }
                }
            }
            if (!updated) {
                this.add(item)
            }
            return this.sortedBy { it.varighet }.toList()
        }

        private fun sanityzeOrgnr(jsonElement: String): String = jsonElement.replace("[0-9]{9}".toRegex(), "*********")
    }
}
