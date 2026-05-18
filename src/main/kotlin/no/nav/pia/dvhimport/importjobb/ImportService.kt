package no.nav.pia.dvhimport.importjobb

import ia.felles.definisjoner.bransjer.Bransje
import ia.felles.definisjoner.bransjer.BransjeId
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import kotlinx.datetime.toJavaLocalDateTime
import kotlinx.datetime.toLocalDateTime
import no.nav.pia.dvhimport.importjobb.domene.BransjeSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.DvhMetadata
import no.nav.pia.dvhimport.importjobb.domene.LandSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.NæringSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.NæringskodeSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.SektorSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori
import no.nav.pia.dvhimport.importjobb.domene.StatistikkUtils
import no.nav.pia.dvhimport.importjobb.domene.Sykefraværsstatistikk
import no.nav.pia.dvhimport.importjobb.domene.TapteDagsverkPerVarighetDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetMetadataDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.tilListe
import no.nav.pia.dvhimport.importjobb.domene.toSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal
import no.nav.pia.dvhimport.importjobb.publiseringsdato.LagreResultat
import no.nav.pia.dvhimport.importjobb.publiseringsdato.NestePubliseringsdato
import no.nav.pia.dvhimport.importjobb.publiseringsdato.Publiseringsdato.Companion.antallDagerTilPubliseringsdato
import no.nav.pia.dvhimport.importjobb.publiseringsdato.Publiseringsdato.Companion.erFørPubliseringsdato
import no.nav.pia.dvhimport.importjobb.publiseringsdato.Publiseringsdato.Companion.sjekkPubliseringErIDag
import no.nav.pia.dvhimport.importjobb.publiseringsdato.Publiseringsdato.Companion.timeZone
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoFraDvhDto
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.importjobb.publiseringsdato.tilPubliseringsdato
import no.nav.pia.dvhimport.importjobb.publiseringsdato.tilPubliseringsdatoKafkaDto
import no.nav.pia.dvhimport.importjobb.publiseringsdato.tilPubliseringsdatoFraDvhDto
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.PubliseringsdatoMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.VirksomhetMetadataMelding
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import no.nav.pia.dvhimport.storage.BucketKlient
import no.nav.pia.dvhimport.storage.BucketKlient.Companion.prosesserIBiter
import no.nav.pia.dvhimport.storage.BucketKlient.Companion.streamVirksomhetMetadata
import no.nav.pia.dvhimport.storage.BucketKlient.Companion.streamVirksomhetSykefraværsstatistikk
import no.nav.pia.dvhimport.storage.Mappestruktur.Companion.tilMappestruktur
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import java.math.RoundingMode
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicReference

class ImportService(
    private val bucketKlient: BucketKlient,
    private val brukÅrOgKvartalIPathTilFilene: Boolean,
    private val publiseringsdatoRepository: PubliseringsdatoRepository? = null,
    private val dryRun: Boolean = false,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val eksportProdusent by lazy {
        EksportProdusent(kafkaConfig = KafkaConfig(), dryRun = dryRun)
    }

    fun sjekkPubliseringsdatoOgStartImport(dato: LocalDate = LocalDate.now()) {
        if (publiseringsdatoRepository == null) {
            logger.error("Kan ikke sjekke publiseringsdato uten database")
            return
        }
        val uprosesserte = publiseringsdatoRepository.hentUprosesserteForDato(dato)
        if (uprosesserte.isEmpty()) {
            logger.info("Ikke publiseringsdato i dag ($dato), ingen import kjøres")
            return
        }
        uprosesserte.forEach { rad ->
            val kvartal = ÅrstallOgKvartal(årstall = rad.årstall, kvartal = rad.kvartal)
            logger.info("Publiseringsdato i dag for $kvartal, starter import")
            importAlleStatistikkKategorier(kvartal)
            publiseringsdatoRepository.markerSomProsessert(rad.id)
            logger.info("Import ferdig for $kvartal, markert som prosessert")
        }
    }

    fun importAlleStatistikkKategorier(
        årstallOgKvartal: ÅrstallOgKvartal,
        startFra: StatistikkKategori? = null,
    ) {
        logger.info("Starter import av sykefraværsstatistikk for alle statistikkkategorier")

        if (!bucketKlient.sjekkBucketExists()) {
            throw IllegalStateException("Bucket ikke funnet, avbryter import for alle kategorier")
        }

        val kategorier = if (startFra != null) {
            logger.info("Starter fra kategori '$startFra', hopper over tidligere kategorier")
            StatistikkKategori.entries.dropWhile { it != startFra }
        } else {
            StatistikkKategori.entries
        }

        val oppsummering = mutableMapOf<String, Int>()

        for (statistikkKategori in kategorier) {
            val antall = importForStatistikkKategori(kategori = statistikkKategori, årstallOgKvartal = årstallOgKvartal)
            oppsummering[statistikkKategori.name] = antall
            logger.info("Ferdig med import av '$statistikkKategori' ($antall rader)")
        }

        logger.info("Starter import av virksomhet metadata")
        val antallMetadata = importVirksomhetMetadata(årstallOgKvartal = årstallOgKvartal)
        oppsummering["VIRKSOMHET_METADATA"] = antallMetadata
        logger.info("Ferdig med import av virksomhet metadata ($antallMetadata rader)")

        val oppsummeringStr = oppsummering.entries.joinToString(", ") { "${it.key}=${it.value}" }
        logger.info("Import ferdig for $årstallOgKvartal: $oppsummeringStr")
    }

    fun importPubliseringsdatoer() {
        val inneværendeÅr = LocalDate.now().year
        val årstall = listOf(inneværendeÅr, inneværendeÅr + 1)
        logger.info("Starter import av publiseringsdatoer for årstall $årstall")

        if (!bucketKlient.sjekkBucketExists()) {
            throw IllegalStateException("Bucket ikke funnet, avbryter import av publiseringsdatoer")
        }

        val antall = importPubliseringsdatoOgSendTilKafka(årstall = årstall)
        logger.info("Import av publiseringsdatoer ferdig ($antall rader)")
    }

    fun importVirksomhetMetadata(årstallOgKvartal: ÅrstallOgKvartal): Int {
        logger.info("Starter import av virksomhet metadata")

        if (!bucketKlient.sjekkBucketExists()) {
            throw IllegalStateException("Bucket ikke funnet, avbryter import av virksomhet metadata")
        }

        val path = årstallOgKvartal.tilMappestruktur(brukÅrOgKvartalIPathTilFilene).pathTilKvartalsvisData()
        return importVirksomhetMetadataOgSendTilKafka(
            path = path,
            årstallOgKvartal = årstallOgKvartal,
        )
    }

    fun importForStatistikkKategori(
        kategori: StatistikkKategori,
        årstallOgKvartal: ÅrstallOgKvartal,
    ): Int {
        logger.info("Starter import av sykefraværsstatistikk for kategori '$kategori'")

        if (!bucketKlient.sjekkBucketExists()) {
            throw IllegalStateException("Bucket ikke funnet, avbryter import for kategori '$kategori'")
        }

        val path = årstallOgKvartal.tilMappestruktur(brukÅrOgKvartalIPathTilFilene).pathTilKvartalsvisData()

        return when (kategori) {
            StatistikkKategori.LAND -> {
                import<LandSykefraværsstatistikkDto>(
                    kategori = StatistikkKategori.LAND,
                    path = path,
                ).also {
                    sendTilKafka(
                        årstallOgKvartal = årstallOgKvartal,
                        statistikk = it,
                        kategori = kategori,
                    )
                }.size
            }

            StatistikkKategori.SEKTOR -> {
                import<SektorSykefraværsstatistikkDto>(
                    kategori = StatistikkKategori.SEKTOR,
                    path = path,
                ).also {
                    sendTilKafka(
                        årstallOgKvartal = årstallOgKvartal,
                        statistikk = it,
                        kategori = kategori,
                    )
                }.size
            }

            StatistikkKategori.NÆRING -> {
                import<NæringSykefraværsstatistikkDto>(
                    kategori = StatistikkKategori.NÆRING,
                    path = path,
                ).also {
                    sendTilKafka(
                        årstallOgKvartal = årstallOgKvartal,
                        statistikk = it,
                        kategori = kategori,
                    )
                }.size
            }

            StatistikkKategori.NÆRINGSKODE -> {
                import<NæringskodeSykefraværsstatistikkDto>(
                    kategori = StatistikkKategori.NÆRINGSKODE,
                    path = path,
                ).also {
                    sendTilKafka(
                        årstallOgKvartal = årstallOgKvartal,
                        statistikk = it,
                        kategori = kategori,
                    )
                }.size
            }

            StatistikkKategori.BRANSJE -> {
                importBransje(path = path, årstallOgKvartal = årstallOgKvartal).also {
                    sendTilKafka(
                        årstallOgKvartal = årstallOgKvartal,
                        statistikk = it,
                        kategori = kategori,
                    )
                }.size
            }

            StatistikkKategori.VIRKSOMHET -> {
                importStatistikkVirksomhetOgSendTilKafka(
                    path = path,
                    årstallOgKvartal = årstallOgKvartal,
                )
            }
        }
    }

    private fun importStatistikkVirksomhetOgSendTilKafka(
        path: String,
        årstallOgKvartal: ÅrstallOgKvartal,
    ): Int {
        try {
            val sumAntallVirksomheter = AtomicReference(0)

            runBlocking {
                val inputStream: InputStream = bucketKlient.getInputStream(
                    path = path,
                    fileName = tilFilNavn(StatistikkKategori.VIRKSOMHET),
                )
                val virksomhetSykefraværsstatistikk: List<VirksomhetSykefraværsstatistikkDto> =
                    streamVirksomhetSykefraværsstatistikk(inputStream)

                virksomhetSykefraværsstatistikk.prosesserIBiter(størrelse = 1000) { statistikk ->
                    logger.info("Sender ${statistikk.size} statistikk for virksomhet til Kafka")
                    sendTilKafka(
                        årstallOgKvartal = årstallOgKvartal,
                        statistikk,
                        kategori = StatistikkKategori.VIRKSOMHET,
                    )
                    sumAntallVirksomheter.getAndAccumulate(statistikk.size) { x, y -> x + y }
                }
                logger.info("Antall statistikk prosessert for kategori ${StatistikkKategori.VIRKSOMHET.name} er: '$sumAntallVirksomheter'")
                kalkulerOgLoggSykefraværsprosent(
                    StatistikkKategori.VIRKSOMHET,
                    virksomhetSykefraværsstatistikk.filter { it.rectype == DatavarehusRecordType.UNDERENHET.kode },
                )

                inputStream.close()
            }
            return sumAntallVirksomheter.get()
        } catch (ex: Exception) {
            logger.error("Import feilet for kategori '${StatistikkKategori.VIRKSOMHET}'", ex)
            throw ex
        }
    }

    private fun importVirksomhetMetadataOgSendTilKafka(
        path: String,
        årstallOgKvartal: ÅrstallOgKvartal,
    ): Int {
        logger.info("Starter import av virksomhet metadata")
        try {
            val sumAntallMetadata = AtomicReference(0)

            runBlocking {
                val inputStream: InputStream = bucketKlient.getInputStream(
                    path = path,
                    fileName = tilFilNavn(DvhMetadata.VIRKSOMHET_METADATA),
                )
                val virksomhetMetadata: List<VirksomhetMetadataDto> = streamVirksomhetMetadata(inputStream)

                virksomhetMetadata.prosesserIBiter(størrelse = 1000) { metadata ->
                    logger.info("Sender ${metadata.size} virksomhetmetadata til Kafka")
                    sendMetadataTilKafka(
                        årstall = årstallOgKvartal.årstall,
                        kvartal = årstallOgKvartal.kvartal,
                        metadata,
                    )
                    sumAntallMetadata.getAndAccumulate(metadata.size) { x, y -> x + y }
                }
                logger.info("Antall metadata prosessert for kategori ${DvhMetadata.VIRKSOMHET_METADATA.name} er: '$sumAntallMetadata'")
            }
            return sumAntallMetadata.get()
        } catch (ex: Exception) {
            logger.error("Import feilet for kategori '${DvhMetadata.VIRKSOMHET_METADATA}'", ex)
            throw ex
        }
    }

    private fun importPubliseringsdatoOgSendTilKafka(årstall: List<Int>): Int {
        val iDag = Clock.System.now().toLocalDateTime(timeZone)
        val publiseringsdatoer = årstall
            .flatMap { år -> importPubliseringsdatoForÅr(år) }
            .distinctBy { it.rapportPeriode }

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

        var antallEndret = 0
        publiseringsdatoer.forEach { dvhDto ->
            val parsed = dvhDto.tilPubliseringsdato()
            val dato = dvhDto.offentligDato.toJavaLocalDateTime().toLocalDate()

            val resultat = publiseringsdatoRepository?.lagrePubliseringsdato(
                årstall = parsed.årstall,
                kvartal = parsed.kvartal,
                dato = dato,
            )

            when (resultat) {
                LagreResultat.NY -> {
                    logger.info("Ny publiseringsdato oppdaget for ${parsed.årstall}-Q${parsed.kvartal}: $dato")
                }
                LagreResultat.OPPDATERT -> {
                    logger.warn("Publiseringsdato endret for ${parsed.årstall}-Q${parsed.kvartal}: ny dato=$dato")
                }
                LagreResultat.UENDRET -> {
                    logger.info("Publiseringsdato uendret for ${parsed.årstall}-Q${parsed.kvartal}: $dato, hopper over Kafka-sending")
                }
                null -> {}
            }

            if (resultat != LagreResultat.UENDRET) {
                eksportProdusent.sendMelding(
                    melding = PubliseringsdatoMelding(
                        årstall = parsed.årstall,
                        kvartal = parsed.kvartal,
                        publiseringsdato = dvhDto.tilPubliseringsdatoKafkaDto(),
                    ),
                )
                antallEndret++
            }
        }

        if (antallEndret > 0) {
            eksportProdusent.flushOgSjekkFeil()
        }
        logger.info("Publiseringsdatoer lest: ${publiseringsdatoer.size}, endret/sendt til Kafka: $antallEndret")
        return publiseringsdatoer.size
    }

    private fun importPubliseringsdatoForÅr(årstall: Int): List<PubliseringsdatoFraDvhDto> {
        val path = if (brukÅrOgKvartalIPathTilFilene) "$årstall" else ""
        val filnavn = tilFilNavn(DvhMetadata.PUBLISERINGSDATO)

        if (!bucketKlient.ensureFileExists(path = path, fileName = filnavn)) {
            logger.info("Publiseringsdato-fil for $årstall finnes ikke ennå ($path/$filnavn), hopper over")
            return emptyList()
        }

        return try {
            val publiseringsdatoer = hentInnholdForMetadata(
                path = path,
                kilde = DvhMetadata.PUBLISERINGSDATO,
            )
            logger.info("Antall rader med publiseringsdatoer for $årstall: ${publiseringsdatoer.size}")
            publiseringsdatoer.tilPubliseringsdatoFraDvhDto()
        } catch (e: Exception) {
            logger.error("Import feilet for publiseringsdato ($årstall)", e)
            throw e
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
            kalkulerOgLoggSykefraværsprosent(kategori, sykefraværsstatistikkDtoList)
            return sykefraværsstatistikkDtoList
        } catch (e: Exception) {
            logger.error("Import feilet for kategori '$kategori'", e)
            throw e
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
            kalkulerOgLoggSykefraværsprosent(StatistikkKategori.BRANSJE, sykefraværsstatistikkDtoList)
            return sykefraværsstatistikkDtoList.filterNotNull()
        } catch (e: Exception) {
            logger.error("Import feilet for kategori '${StatistikkKategori.BRANSJE}'", e)
            throw e
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
        val innhold = bucketKlient.getFromFile(
            path = path,
            fileName = filnavn,
        )
        if (innhold.isNullOrEmpty()) {
            throw IllegalStateException("Ingen data funnet for kilde '$kilde' i '$path/$filnavn'")
        }

        val data: List<String> = innhold.tilListe()
        if (data.isEmpty()) {
            throw IllegalStateException("Tom fil for kilde '$kilde' i '$path/$filnavn'")
        }

        logger.info("Antall rader med data for kilde '$kilde' og path '$path': ${data.size}")
        return data
    }

    private fun <T> sendTilKafka(
        årstallOgKvartal: ÅrstallOgKvartal,
        statistikk: List<T>,
        kategori: StatistikkKategori,
    ) {
        logger.info("Sender ${statistikk.size} statistikk for kategori $kategori til Kafka")
        statistikk.forEach {
            eksportProdusent.sendMelding(
                melding = SykefraværsstatistikkMelding(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    sykefraværsstatistikk = it,
                ),
            )
        }
        eksportProdusent.flushOgSjekkFeil()
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
        eksportProdusent.flushOgSjekkFeil()
    }

    companion object {
        const val ANTALL_SIFRE_I_UTREGNING = 3
        const val ANTALL_SIFRE_I_RESULTAT = 1

        private val logger: Logger = LoggerFactory.getLogger(this::class.java)

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

        fun kalkulerOgLoggSykefraværsprosent(
            kategori: StatistikkKategori,
            statistikk: List<Sykefraværsstatistikk?>,
        ): BigDecimal {
            val sumAntallTapteDagsverk = statistikk.sumOf { it?.tapteDagsverk ?: ZERO }
            val sumAntallMuligeDagsverk = statistikk.sumOf { it?.muligeDagsverk ?: ZERO }
            val sykefraværsprosentForKategori =
                StatistikkUtils.kalkulerSykefraværsprosent(sumAntallTapteDagsverk, sumAntallMuligeDagsverk)
            logger.info("Sykefraværsprosent -snitt- for kategori ${kategori.name} er: '$sykefraværsprosentForKategori'")
            return sykefraværsprosentForKategori
        }

        fun nestePubliseringsdato(
            publiseringsdatoer: List<PubliseringsdatoFraDvhDto>,
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

        enum class DatavarehusRecordType(
            val kode: String,
        ) {
            OVERORDNET_ENHET("1"),
            UNDERENHET("2"),
            ORGLED("3"),
        }
    }
}
