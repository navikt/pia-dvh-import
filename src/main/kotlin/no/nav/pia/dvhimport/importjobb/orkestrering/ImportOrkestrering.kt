package no.nav.pia.dvhimport.importjobb.orkestrering

import no.nav.pia.dvhimport.importjobb.ImportService
import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.varsling.SlackVarsler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.temporal.ChronoUnit

class ImportOrkestrering(
    private val importService: ImportService,
    private val lockRepository: ImportLockRepository,
    private val stegRepository: ImportStegRepository,
    private val publiseringsdatoRepository: PubliseringsdatoRepository,
    private val slackVarsler: SlackVarsler,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    fun kjørImportForPubliseringsdato(
        dato: LocalDate = LocalDate.now(),
        dryRun: Boolean,
    ) {
        val uprosessert = publiseringsdatoRepository.hentUprosessertForDato(dato)
        if (uprosessert == null) {
            logger.info("Ikke publiseringsdato i dag ($dato), ingen import kjøres")
            varsleOmKommendePubliseringsdato(dato)
            return
        }
        val årstallOgKvartal = ÅrstallOgKvartal(årstall = uprosessert.årstall, kvartal = uprosessert.kvartal)
        kjørImport(publiseringsdatoId = uprosessert.id, årstallOgKvartal = årstallOgKvartal, dryRun = dryRun)
    }

    fun kjørImportForKvartal(
        årstallOgKvartal: ÅrstallOgKvartal,
        dryRun: Boolean,
    ) {
        val publiseringsdatoId = publiseringsdatoRepository.hentIdForKvartal(
            årstall = årstallOgKvartal.årstall,
            kvartal = årstallOgKvartal.kvartal,
        )
            ?: throw IllegalStateException("Ingen publiseringsdato funnet for $årstallOgKvartal, kan ikke starte orkestrert import")
        kjørImport(publiseringsdatoId = publiseringsdatoId, årstallOgKvartal = årstallOgKvartal, dryRun = dryRun)
    }

    fun kjørImport(
        publiseringsdatoId: Int,
        årstallOgKvartal: ÅrstallOgKvartal,
        dryRun: Boolean,
    ) {
        val markør = if (dryRun) " — 🧪 dry-run (ingen Kafka)" else ""
        val eksisterendeLås = lockRepository.hentForPubliseringsdato(publiseringsdatoId)
        when (eksisterendeLås?.status) {
            null -> {
                if (lockRepository.taLås(publiseringsdatoId) == null) {
                    logger.info("En annen kjøring holder allerede låsen for $årstallOgKvartal, avbryter")
                    return
                }
                slackVarsler.send("📥 Import startet for $årstallOgKvartal$markør")
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
                slackVarsler.send("🔄 Import gjenopptatt for $årstallOgKvartal$markør")
            }
        }

        stegRepository.opprettStegHvisIkkeFinnes(publiseringsdatoId)

        try {
            validerAlleSteg(publiseringsdatoId, årstallOgKvartal)
            sendAlleSteg(publiseringsdatoId, årstallOgKvartal, dryRun)
            lockRepository.markerFerdig(publiseringsdatoId)
            publiseringsdatoRepository.markerSomProsessert(publiseringsdatoId)
            logger.info("Import ferdig for $årstallOgKvartal")
            slackVarsler.send("🎉 ${if (dryRun) "Dry-run" else "Import"} ferdig for $årstallOgKvartal")
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
                logger.error("Validering feilet på ${steg.visningsnavn} (${e.kontroll}) for $årstallOgKvartal: ${e.message}")
                slackVarsler.send("❌ Import feilet på ${steg.visningsnavn} (${e.kontroll}) for $årstallOgKvartal: ${e.message}")
                throw e
            } catch (e: Exception) {
                stegRepository.markerFeilet(publiseringsdatoId, steg, Kontroll.ANNET)
                logger.error("Uventet feil på ${steg.visningsnavn} for $årstallOgKvartal: ${e.message}", e)
                slackVarsler.send("❌ Import feilet på ${steg.visningsnavn} (uventet feil) for $årstallOgKvartal: ${e.message}")
                throw e
            }
        }
        slackVarsler.send("✅ Alle kategorier validert for $årstallOgKvartal — starter sending")
    }

    private fun sendAlleSteg(
        publiseringsdatoId: Int,
        årstallOgKvartal: ÅrstallOgKvartal,
        dryRun: Boolean,
    ) {
        val gjenstår = stegRepository.hentAlle(publiseringsdatoId)
            .filter { it.status != ImportStegStatus.FERDIG }
            .map { it.steg }

        gjenstår.forEach { steg ->
            try {
                val antallSendt = importService.sendSteg(steg, årstallOgKvartal, dryRun)
                stegRepository.markerFerdig(publiseringsdatoId, steg, antallSendt)
                slackVarsler.send("✅ ${steg.visningsnavn} ferdig")
            } catch (e: Exception) {
                stegRepository.markerFeilet(publiseringsdatoId, steg, Kontroll.KAFKA_ERROR)
                throw e
            }
        }
    }

    private fun varsleOmKommendePubliseringsdato(dato: LocalDate) {
        val neste = publiseringsdatoRepository.hentNesteUprosessertePubliseringsdato(dato) ?: return
        val dagerTil = ChronoUnit.DAYS.between(dato, neste.dato)
        val tekst = when (dagerTil) {
            7L -> "📅 1 uke til publiseringsdato"
            3L -> "📅 3 dager til publiseringsdato"
            2L -> "📅 2 dager til publiseringsdato"
            1L -> "📅 1 dag til publiseringsdato"
            else -> return
        }
        val kvartal = ÅrstallOgKvartal(årstall = neste.årstall, kvartal = neste.kvartal)
        slackVarsler.send("$tekst ($kvartal)")
    }
}
