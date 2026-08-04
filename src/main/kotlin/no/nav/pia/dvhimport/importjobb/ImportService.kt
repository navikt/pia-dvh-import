package no.nav.pia.dvhimport.importjobb

import ia.felles.definisjoner.bransjer.BransjeId
import ia.felles.definisjoner.bransjer.BransjeSN2007
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import kotlinx.datetime.toJavaLocalDateTime
import kotlinx.datetime.toLocalDateTime
import no.nav.pia.dvhimport.importjobb.domene.BransjeSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.DvhMetadata
import no.nav.pia.dvhimport.importjobb.domene.HarÅrstallOgKvartal
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
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.PubliseringsdatoMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.SykefraværsstatistikkMelding
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.VirksomhetMetadataMelding
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportSteg
import no.nav.pia.dvhimport.importjobb.orkestrering.Kontroll
import no.nav.pia.dvhimport.importjobb.orkestrering.Radgrenser
import no.nav.pia.dvhimport.importjobb.orkestrering.StegValideringsresultat
import no.nav.pia.dvhimport.importjobb.orkestrering.ValideringsfeilException
import no.nav.pia.dvhimport.importjobb.publiseringsdato.LagreResultat
import no.nav.pia.dvhimport.importjobb.publiseringsdato.NestePubliseringsdato
import no.nav.pia.dvhimport.importjobb.publiseringsdato.Publiseringsdato.Companion.antallDagerTilPubliseringsdato
import no.nav.pia.dvhimport.importjobb.publiseringsdato.Publiseringsdato.Companion.erFørPubliseringsdato
import no.nav.pia.dvhimport.importjobb.publiseringsdato.Publiseringsdato.Companion.sjekkPubliseringErIDag
import no.nav.pia.dvhimport.importjobb.publiseringsdato.Publiseringsdato.Companion.timeZone
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoFraDvhDto
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.importjobb.publiseringsdato.tilPubliseringsdato
import no.nav.pia.dvhimport.importjobb.publiseringsdato.tilPubliseringsdatoFraDvhDto
import no.nav.pia.dvhimport.importjobb.publiseringsdato.tilPubliseringsdatoKafkaDto
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
    private val publiseringsdatoRepository: PubliseringsdatoRepository,
    private val radgrenser: Radgrenser,
    private val skalValidereSfProsent: Boolean = true,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private val eksportProdusent by lazy {
        EksportProdusent(kafkaConfig = KafkaConfig())
    }

    fun importPubliseringsdatoer(dryRun: Boolean) {
        val inneværendeÅr = LocalDate.now().year
        val årstall = listOf(inneværendeÅr, inneværendeÅr + 1)
        logger.info("Starter import av publiseringsdatoer for årstall $årstall")

        if (!bucketKlient.sjekkBucketExists()) {
            throw IllegalStateException("Bucket ikke funnet, avbryter import av publiseringsdatoer")
        }

        val antall = importPubliseringsdatoOgSendTilKafka(årstall = årstall, dryRun = dryRun)
        logger.info("Import av publiseringsdatoer ferdig ($antall rader)")
    }

    fun lesOgValiderSteg(
        steg: ImportSteg,
        årstallOgKvartal: ÅrstallOgKvartal,
        landSfProsent: BigDecimal?,
    ): StegValideringsresultat {
        logger.info("Validerer steg '$steg' for $årstallOgKvartal")
        if (!bucketKlient.sjekkBucketExists()) {
            throw IllegalStateException("Bucket ikke funnet, avbryter validering av steg '$steg'")
        }
        val path = årstallOgKvartal.tilMappestruktur(brukÅrOgKvartalIPathTilFilene).pathTilKvartalsvisData()

        return when (steg) {
            ImportSteg.IMPORT_LAND -> {
                val data = import<LandSykefraværsstatistikkDto>(StatistikkKategori.LAND, path)
                validerStruktur(steg, data) { it.land }
                validerÅrstall(steg, data, årstallOgKvartal)
                validerRadgrense(steg, data.size)
                val beregnet = kalkulerOgLoggSykefraværsprosent(StatistikkKategori.LAND, data)
                // LAND er referansen: beregnet sf_prosent skal stemme med prosent-feltet i fila.
                validerSfProsent(steg, beregnet, data.first().prosent, årstallOgKvartal)
                StegValideringsresultat(data.size, beregnet)
            }

            ImportSteg.IMPORT_SEKTOR -> {
                val data = import<SektorSykefraværsstatistikkDto>(StatistikkKategori.SEKTOR, path)
                validerStruktur(steg, data) { it.sektor }
                validerÅrstall(steg, data, årstallOgKvartal)
                validerRadgrense(steg, data.size)
                val beregnet = kalkulerOgLoggSykefraværsprosent(StatistikkKategori.SEKTOR, data)
                validerSfProsent(steg, beregnet, landSfProsent, årstallOgKvartal)
                StegValideringsresultat(data.size, beregnet)
            }

            ImportSteg.IMPORT_NARING -> {
                val data = import<NæringSykefraværsstatistikkDto>(StatistikkKategori.NÆRING, path)
                validerStruktur(steg, data) { it.næring }
                validerÅrstall(steg, data, årstallOgKvartal)
                validerRadgrense(steg, data.size)
                val beregnet = kalkulerOgLoggSykefraværsprosent(StatistikkKategori.NÆRING, data)
                validerSfProsent(steg, beregnet, landSfProsent, årstallOgKvartal)
                StegValideringsresultat(data.size, beregnet)
            }

            ImportSteg.IMPORT_NARINGSKODE -> {
                val data = import<NæringskodeSykefraværsstatistikkDto>(StatistikkKategori.NÆRINGSKODE, path)
                validerStruktur(steg, data) { it.næringskode }
                validerÅrstall(steg, data, årstallOgKvartal)
                validerRadgrense(steg, data.size)
                val beregnet = kalkulerOgLoggSykefraværsprosent(StatistikkKategori.NÆRINGSKODE, data)
                validerSfProsent(steg, beregnet, landSfProsent, årstallOgKvartal)
                StegValideringsresultat(data.size, beregnet)
            }

            ImportSteg.IMPORT_BRANSJE -> {
                // Bransje utledes fra næring/næringskode. Struktur, årstall og radgrense er allerede
                // dekket av NÆRING- og NÆRINGSKODE-stegene, og bransje er unntatt sf_prosent-sjekken.
                val næringData = import<NæringSykefraværsstatistikkDto>(StatistikkKategori.NÆRING, path)
                val bransjeData = importBransje(path, årstallOgKvartal)
                StegValideringsresultat(
                    næringData.size,
                    kalkulerOgLoggSykefraværsprosent(StatistikkKategori.BRANSJE, bransjeData),
                )
            }

            ImportSteg.IMPORT_VIRKSOMHET -> {
                validerVirksomhet(steg, path, årstallOgKvartal, landSfProsent)
            }

            ImportSteg.IMPORT_VIRKSOMHET_METADATA -> {
                validerVirksomhetMetadata(steg, path, årstallOgKvartal)
            }
        }
    }

    fun sendSteg(
        steg: ImportSteg,
        årstallOgKvartal: ÅrstallOgKvartal,
        dryRun: Boolean,
    ): Int {
        logger.info("Sender steg '$steg' for $årstallOgKvartal")
        if (!bucketKlient.sjekkBucketExists()) {
            throw IllegalStateException("Bucket ikke funnet, avbryter sending av steg '$steg'")
        }
        val path = årstallOgKvartal.tilMappestruktur(brukÅrOgKvartalIPathTilFilene).pathTilKvartalsvisData()

        return when (steg) {
            ImportSteg.IMPORT_LAND -> {
                val data = import<LandSykefraværsstatistikkDto>(StatistikkKategori.LAND, path)
                sendTilKafka(årstallOgKvartal, data, StatistikkKategori.LAND, dryRun)
                data.size
            }

            ImportSteg.IMPORT_SEKTOR -> {
                val data = import<SektorSykefraværsstatistikkDto>(StatistikkKategori.SEKTOR, path)
                sendTilKafka(årstallOgKvartal, data, StatistikkKategori.SEKTOR, dryRun)
                data.size
            }

            ImportSteg.IMPORT_NARING -> {
                val data = import<NæringSykefraværsstatistikkDto>(StatistikkKategori.NÆRING, path)
                sendTilKafka(årstallOgKvartal, data, StatistikkKategori.NÆRING, dryRun)
                data.size
            }

            ImportSteg.IMPORT_NARINGSKODE -> {
                val data = import<NæringskodeSykefraværsstatistikkDto>(StatistikkKategori.NÆRINGSKODE, path)
                sendTilKafka(årstallOgKvartal, data, StatistikkKategori.NÆRINGSKODE, dryRun)
                data.size
            }

            ImportSteg.IMPORT_BRANSJE -> {
                val data = importBransje(path, årstallOgKvartal)
                sendTilKafka(årstallOgKvartal, data, StatistikkKategori.BRANSJE, dryRun)
                data.size
            }

            ImportSteg.IMPORT_VIRKSOMHET -> {
                importStatistikkVirksomhetOgSendTilKafka(path, årstallOgKvartal, dryRun)
            }

            ImportSteg.IMPORT_VIRKSOMHET_METADATA -> {
                importVirksomhetMetadataOgSendTilKafka(
                    path,
                    årstallOgKvartal,
                    dryRun,
                )
            }
        }
    }

    private fun <T> validerStruktur(
        steg: ImportSteg,
        data: List<T>,
        felt: (T) -> String,
    ) {
        val regex = steg.strukturRegex ?: return
        val antallUgyldige = data.count { !regex.matches(felt(it)) }
        if (antallUgyldige > 0) {
            throw ValideringsfeilException(
                Kontroll.FEIL_STRUKTUR_I_INPUT_FIL,
                "Steg $steg: $antallUgyldige rader bryter strukturkravet '${regex.pattern}'",
            )
        }
    }

    private fun validerÅrstall(
        steg: ImportSteg,
        data: List<HarÅrstallOgKvartal>,
        årstallOgKvartal: ÅrstallOgKvartal,
    ) {
        val antallAvvik =
            data.count { it.årstall != årstallOgKvartal.årstall || it.kvartal != årstallOgKvartal.kvartal }
        if (antallAvvik > 0) {
            throw ValideringsfeilException(
                Kontroll.FEIL_ÅRSTALL_ELLER_KVARTAL,
                "Steg $steg: $antallAvvik rader har feil årstall/kvartal (forventet $årstallOgKvartal)",
            )
        }
    }

    private fun validerRadgrense(
        steg: ImportSteg,
        antall: Int,
    ) {
        val grense = radgrenser.forSteg(steg)
        if (!grense.inneholder(antall)) {
            throw ValideringsfeilException(
                Kontroll.FEIL_ANTALL_RADER_I_INPUT_FIL,
                "Steg $steg: $antall rader utenfor tillatt intervall [${grense.nedre}, ${grense.øvre}]",
            )
        }
    }

    private fun validerSfProsent(
        steg: ImportSteg,
        beregnet: BigDecimal,
        referanse: BigDecimal?,
        årstallOgKvartal: ÅrstallOgKvartal,
    ) {
        // Avrund begge til 1 desimal og sammenlign; like verdier = gyldig steg
        val a = beregnet.setScale(ANTALL_SIFRE_I_RESULTAT, RoundingMode.HALF_UP)
        val b = referanse?.setScale(ANTALL_SIFRE_I_RESULTAT, RoundingMode.HALF_UP)
        if (b != null && a.compareTo(b) == 0) {
            return
        }
        // Her har vi et avvik: enten mangler LAND-referansen, eller sf-prosenten er ulik den
        val detalj = if (b == null) {
            "Steg $steg: mangler referanse-sykefraværsprosent (LAND ikke validert)"
        } else {
            "Steg $steg: sykefraværsprosent $a avviker fra referanse $b"
        }
        if (!skalValidereSfProsent) {
            logger.info(
                "ℹ️ Import ville ha feilet på ${steg.visningsnavn} (SF_PROSENT_FEIL) for $årstallOgKvartal: " +
                    "$detalj, MEN ettersom vi er i dev fortsetter vi importen",
            )
            return
        }
        throw ValideringsfeilException(Kontroll.SF_PROSENT_FEIL, detalj)
    }

    private fun <T> filtrerPåOrgnr(
        steg: ImportSteg,
        data: List<T>,
        orgnr: (T) -> String,
    ): List<T> {
        val regex = steg.strukturRegex ?: return data
        val gyldige = data.filter { regex.matches(orgnr(it)) }
        val droppet = data.size - gyldige.size
        if (droppet > 0) {
            logger.info("Steg $steg: filtrerte bort $droppet rader med ugyldig orgnr (av ${data.size})")
        }
        return gyldige
    }

    private fun validerVirksomhet(
        steg: ImportSteg,
        path: String,
        årstallOgKvartal: ÅrstallOgKvartal,
        landSfProsent: BigDecimal?,
    ): StegValideringsresultat =
        runBlocking {
            val inputStream: InputStream = bucketKlient.getInputStream(
                path = path,
                fileName = tilFilNavn(StatistikkKategori.VIRKSOMHET),
            )
            val alle: List<VirksomhetSykefraværsstatistikkDto> = streamVirksomhetSykefraværsstatistikk(inputStream)
            val gyldige = filtrerPåOrgnr(steg, alle) { it.orgnr }
            validerÅrstall(steg, gyldige, årstallOgKvartal)
            validerRadgrense(steg, gyldige.size)
            val beregnet = kalkulerOgLoggSykefraværsprosent(
                StatistikkKategori.VIRKSOMHET,
                gyldige.filter { it.rectype == DatavarehusRecordType.UNDERENHET.kode },
            )
            validerSfProsent(steg, beregnet, landSfProsent, årstallOgKvartal)
            inputStream.close()
            StegValideringsresultat(gyldige.size, beregnet)
        }

    private fun validerVirksomhetMetadata(
        steg: ImportSteg,
        path: String,
        årstallOgKvartal: ÅrstallOgKvartal,
    ): StegValideringsresultat =
        runBlocking {
            val inputStream: InputStream = bucketKlient.getInputStream(
                path = path,
                fileName = tilFilNavn(DvhMetadata.VIRKSOMHET_METADATA),
            )
            val alle: List<VirksomhetMetadataDto> = streamVirksomhetMetadata(inputStream)
            val gyldige = filtrerPåOrgnr(steg, alle) { it.orgnr }
            validerÅrstall(steg, gyldige, årstallOgKvartal)
            validerRadgrense(steg, gyldige.size)
            inputStream.close()
            StegValideringsresultat(gyldige.size, null)
        }

    private fun importStatistikkVirksomhetOgSendTilKafka(
        path: String,
        årstallOgKvartal: ÅrstallOgKvartal,
        dryRun: Boolean,
    ): Int {
        try {
            val sumAntallVirksomheter = AtomicReference(0)

            runBlocking {
                val inputStream: InputStream = bucketKlient.getInputStream(
                    path = path,
                    fileName = tilFilNavn(StatistikkKategori.VIRKSOMHET),
                )
                val virksomhetSykefraværsstatistikk: List<VirksomhetSykefraværsstatistikkDto> =
                    filtrerPåOrgnr(
                        ImportSteg.IMPORT_VIRKSOMHET,
                        streamVirksomhetSykefraværsstatistikk(inputStream),
                    ) { it.orgnr }

                virksomhetSykefraværsstatistikk.prosesserIBiter(størrelse = 1000) { statistikk ->
                    logger.info("Sender ${statistikk.size} statistikk for virksomhet til Kafka")
                    sendTilKafka(
                        årstallOgKvartal = årstallOgKvartal,
                        statistikk,
                        kategori = StatistikkKategori.VIRKSOMHET,
                        dryRun = dryRun,
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
        dryRun: Boolean,
    ): Int {
        logger.info("Starter import av virksomhet metadata")
        try {
            val sumAntallMetadata = AtomicReference(0)

            runBlocking {
                val inputStream: InputStream = bucketKlient.getInputStream(
                    path = path,
                    fileName = tilFilNavn(DvhMetadata.VIRKSOMHET_METADATA),
                )
                val virksomhetMetadata: List<VirksomhetMetadataDto> =
                    filtrerPåOrgnr(
                        ImportSteg.IMPORT_VIRKSOMHET_METADATA,
                        streamVirksomhetMetadata(inputStream),
                    ) { it.orgnr }

                virksomhetMetadata.prosesserIBiter(størrelse = 1000) { metadata ->
                    logger.info("Sender ${metadata.size} virksomhetmetadata til Kafka")
                    sendMetadataTilKafka(
                        årstall = årstallOgKvartal.årstall,
                        kvartal = årstallOgKvartal.kvartal,
                        metadata,
                        dryRun = dryRun,
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

    private fun importPubliseringsdatoOgSendTilKafka(
        årstall: List<Int>,
        dryRun: Boolean,
    ): Int {
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

            val resultat = publiseringsdatoRepository.lagrePubliseringsdato(
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
            }

            if (resultat != LagreResultat.UENDRET) {
                eksportProdusent.sendMelding(
                    melding = PubliseringsdatoMelding(
                        årstall = parsed.årstall,
                        kvartal = parsed.kvartal,
                        publiseringsdato = dvhDto.tilPubliseringsdatoKafkaDto(),
                    ),
                    dryRun = dryRun,
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

            val sykefraværsstatistikkDtoList: List<BransjeSykefraværsstatistikkDto?> =
                BransjeSN2007.entries.map { bransje ->
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
            kalkulerOgLoggSykefraværsprosent(kategori = StatistikkKategori.BRANSJE, statistikk = sykefraværsstatistikkDtoList)
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
        dryRun: Boolean,
    ) {
        if (dryRun) {
            logger.info("DRY_RUN: hopper over Kafka-sending for kategori $kategori (${statistikk.size} meldinger)")
            return
        }
        logger.info("Sender ${statistikk.size} statistikk for kategori $kategori til Kafka")
        statistikk.forEach {
            eksportProdusent.sendMelding(
                melding = SykefraværsstatistikkMelding(
                    årstall = årstallOgKvartal.årstall,
                    kvartal = årstallOgKvartal.kvartal,
                    sykefraværsstatistikk = it,
                ),
                dryRun = dryRun,
            )
        }
        eksportProdusent.flushOgSjekkFeil()
    }

    private fun sendMetadataTilKafka(
        årstall: Int,
        kvartal: Int,
        metadata: List<VirksomhetMetadataDto>,
        dryRun: Boolean,
    ) {
        if (dryRun) {
            logger.info("DRY_RUN: hopper over Kafka-sending for kategori VIRKSOMHET_METADATA (${metadata.size} meldinger)")
            return
        }
        metadata.forEach {
            val metadataMelding = VirksomhetMetadataMelding(
                årstall = årstall,
                kvartal = kvartal,
                virksomhetMetadata = it,
            )
            eksportProdusent.sendMelding(
                melding = metadataMelding,
                dryRun = dryRun,
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
            bransje: BransjeSN2007,
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
