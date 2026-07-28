package no.nav.pia.dvhimport.importjobb.orkestrering

import no.nav.pia.dvhimport.importjobb.ImportService
import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.time.LocalDate

class ImportOrkestrering(
    private val importService: ImportService,
    private val lockRepository: ImportLockRepository,
    private val stegRepository: ImportStegRepository,
    private val publiseringsdatoRepository: PubliseringsdatoRepository,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    fun kjørImportForPubliseringsdato(
        dato: LocalDate = LocalDate.now(),
        dryRun: Boolean = false,
    ) {
        val uprosessert = publiseringsdatoRepository.hentUprosessertForDato(dato)
        if (uprosessert == null) {
            logger.info("Ikke publiseringsdato i dag ($dato), ingen import kjøres")
            return
        }
        val årstallOgKvartal = ÅrstallOgKvartal(årstall = uprosessert.årstall, kvartal = uprosessert.kvartal)
        if (dryRun) {
            kjørDryRun(årstallOgKvartal)
            return
        }
        kjørImport(publiseringsdatoId = uprosessert.id, årstallOgKvartal = årstallOgKvartal)
    }

    fun kjørImportForKvartal(
        årstallOgKvartal: ÅrstallOgKvartal,
        dryRun: Boolean = false,
    ) {
        if (dryRun) {
            kjørDryRun(årstallOgKvartal)
            return
        }
        val publiseringsdatoId = publiseringsdatoRepository.hentIdForKvartal(
            årstall = årstallOgKvartal.årstall,
            kvartal = årstallOgKvartal.kvartal,
        )
            ?: throw IllegalStateException("Ingen publiseringsdato funnet for $årstallOgKvartal, kan ikke starte orkestrert import")
        kjørImport(publiseringsdatoId = publiseringsdatoId, årstallOgKvartal = årstallOgKvartal)
    }

    /**
     * Dry-run: validerer alle 7 steg mot ekte data uten å sende Kafka og uten å endre DB.
     * Tar ingen lås, oppretter ingen steg-rader, markerer ingenting som prosessert.
     * Fritt re-kjørbar og blokkerer aldri en ekte import. Utfallet går til logg.
     */
    fun kjørDryRun(årstallOgKvartal: ÅrstallOgKvartal) {
        logger.info("DRY_RUN: starter validering for $årstallOgKvartal — ingen lås, ingen DB-endring, ingen Kafka")
        var landSfProsent: BigDecimal? = null
        ImportSteg.iRekkefolge.forEach { steg ->
            val resultat = try {
                importService.lesOgValiderSteg(steg, årstallOgKvartal, landSfProsent)
            } catch (e: ValideringsfeilException) {
                logger.warn("DRY_RUN: validering FEILET på steg $steg (${e.kontroll}): ${e.message} — ville stoppet ekte kjøring")
                return
            }
            if (steg == ImportSteg.IMPORT_LAND) {
                landSfProsent = resultat.sfProsent
            }
            logger.info("DRY_RUN: steg $steg validert (rader=${resultat.antallRaderLest}, sfProsent=${resultat.sfProsent})")
        }
        logger.info("DRY_RUN: validering fullført for $årstallOgKvartal — ingen data sendt")
    }

    fun kjørImport(
        publiseringsdatoId: Int,
        årstallOgKvartal: ÅrstallOgKvartal,
    ) {
        val eksisterendeLås = lockRepository.hentForPubliseringsdato(publiseringsdatoId)
        when (eksisterendeLås?.status) {
            null -> {
                if (lockRepository.taLås(publiseringsdatoId) == null) {
                    logger.info("En annen kjøring holder allerede låsen for $årstallOgKvartal, avbryter")
                    return
                }
            }

            ImportLockStatus.FERDIG -> {
                logger.info("Import er allerede ferdig for $årstallOgKvartal, kjører ikke")
                return
            }

            ImportLockStatus.STARTET -> {
                logger.info("Import pågår allerede for $årstallOgKvartal, kjører ikke")
                return
            }

            ImportLockStatus.FEILET -> {
                logger.info("Gjenopptar feilet import for $årstallOgKvartal")
                lockRepository.markerStartet(publiseringsdatoId)
            }
        }

        stegRepository.opprettStegHvisIkkeFinnes(publiseringsdatoId)

        try {
            validerAlleSteg(publiseringsdatoId, årstallOgKvartal)
            sendAlleSteg(publiseringsdatoId, årstallOgKvartal)
            lockRepository.markerFerdig(publiseringsdatoId)
            publiseringsdatoRepository.markerSomProsessert(publiseringsdatoId)
            logger.info("Import ferdig for $årstallOgKvartal")
        } catch (e: Exception) {
            logger.error("Import feilet for $årstallOgKvartal", e)
            lockRepository.markerFeilet(publiseringsdatoId)
            throw e
        }
    }

    private fun validerAlleSteg(
        publiseringsdatoId: Int,
        årstallOgKvartal: ÅrstallOgKvartal,
    ) {
        val gjenstår = stegRepository.hentAlle(publiseringsdatoId)
            .filter { it.status != ImportStegStatus.VALIDERT && it.status != ImportStegStatus.FERDIG }
            .map { it.steg }

        gjenstår.forEach { steg ->
            stegRepository.markerStartet(publiseringsdatoId, steg)
            try {
                val landSfProsent = stegRepository.hent(publiseringsdatoId, ImportSteg.IMPORT_LAND)?.sfProsent
                val resultat = importService.lesOgValiderSteg(steg, årstallOgKvartal, landSfProsent)
                stegRepository.markerValidert(publiseringsdatoId, steg, resultat.antallRaderLest, resultat.sfProsent)
            } catch (e: ValideringsfeilException) {
                stegRepository.markerFeilet(publiseringsdatoId, steg, e.kontroll)
                throw e
            } catch (e: Exception) {
                stegRepository.markerFeilet(publiseringsdatoId, steg, Kontroll.ANNET)
                throw e
            }
        }
    }

    private fun sendAlleSteg(
        publiseringsdatoId: Int,
        årstallOgKvartal: ÅrstallOgKvartal,
    ) {
        val gjenstår = stegRepository.hentAlle(publiseringsdatoId)
            .filter { it.status != ImportStegStatus.FERDIG }
            .map { it.steg }

        gjenstår.forEach { steg ->
            try {
                val antallSendt = importService.sendSteg(steg, årstallOgKvartal)
                stegRepository.markerFerdig(publiseringsdatoId, steg, antallSendt)
            } catch (e: Exception) {
                stegRepository.markerFeilet(publiseringsdatoId, steg, Kontroll.KAFKA_ERROR)
                throw e
            }
        }
    }
}
