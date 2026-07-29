package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.integrasjoner.jobbsender.Jobb
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.maps.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.KonsistentTestdata
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportLockRepository
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportLockStatus
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportSteg
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportStegRepository
import no.nav.pia.dvhimport.importjobb.orkestrering.ImportStegStatus
import no.nav.pia.dvhimport.importjobb.orkestrering.Kontroll
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import no.nav.pia.dvhimport.konfigurasjon.createDataSource
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import java.math.BigDecimal
import java.time.LocalDate
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using

class ImportServiceIntegrasjonstest {
    private val gcsContainer = TestContainerHelper.googleCloudStorage
    private val kafkaContainer = TestContainerHelper.kafka

    companion object {
        // Kobler til SAMME postgres som appen, via host-tilgjengelig jdbcUrl (appen bruker nettverks-alias).
        private val testDataSource = createDataSource(
            TestContainerHelper.postgresContainerHelper.container.jdbcUrl +
                "&user=${TestContainerHelper.postgresContainerHelper.container.username}" +
                "&password=${TestContainerHelper.postgresContainerHelper.container.password}",
        )
        private val publiseringsdatoRepository = PubliseringsdatoRepository(testDataSource)
        private val lockRepository = ImportLockRepository(testDataSource)
        private val stegRepository = ImportStegRepository(testDataSource)
    }
    private val eksportertStatistikkKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER)

    private val eksportertVirksomhetStatistikkKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET)

    private val eksportertVirksomhetMetadataKonsument =
        kafkaContainer.nyKonsument(topic = KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET_METADATA)

    @BeforeTest
    fun setup() {
        gcsContainer.opprettTestBucketHvisIkkeFunnet()
        /*
         GCS Rest API er tilgjengelig fra eksponert port (dynamic port) på localhost (kjør test i debug)
         f.eks: http://localhost:{dynamic_port}/storage/v1/b/fake-gcs-bucket-in-container/o/land.json
         */
        eksportertStatistikkKonsument.subscribe(
            mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER.navnMedNamespace),
        )
        eksportertVirksomhetStatistikkKonsument.subscribe(
            mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET.navnMedNamespace),
        )
        eksportertVirksomhetMetadataKonsument.subscribe(
            mutableListOf(KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET_METADATA.navnMedNamespace),
        )
    }

    @AfterTest
    fun tearDown() {
        eksportertStatistikkKonsument.unsubscribe()
        eksportertStatistikkKonsument.close()

        eksportertVirksomhetStatistikkKonsument.unsubscribe()
        eksportertVirksomhetStatistikkKonsument.close()

        eksportertVirksomhetMetadataKonsument.unsubscribe()
        eksportertVirksomhetMetadataKonsument.close()
    }

    @Test
    fun `import statistikk for alle kategorier`() {
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2026, kvartal = 2)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2026-2")

        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori LAND er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori SEKTOR er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRING er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori NÆRINGSKODE er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Sykefraværsprosent -snitt- for kategori VIRKSOMHET er: '6.2'".toRegex()
        dvhImportApplikasjon shouldContainLog "Import ferdig for 2. kvartal 2026".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'alleKategorierSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `dry-run validerer alle steg uten å endre DB eller sende Kafka`() {
        val årstall = 2027
        val kvartal = 1
        val dato = LocalDate.of(2027, 3, 1)
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        val id = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!

        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2027-1:DRY_RUN")

        dvhImportApplikasjon shouldContainLog "DRY_RUN: validering fullført for 1. kvartal 2027".toRegex()

        // Dry-run persisterer ingenting: ingen lås, ingen steg, publiseringsdato fortsatt uprosessert
        lockRepository.hentForPubliseringsdato(id) shouldBe null
        stegRepository.hentAlle(id).shouldBeEmpty()
        publiseringsdatoRepository.hentUprosessertForDato(dato).shouldNotBeNull()
    }

    @Test
    fun `dry-run rapporterer valideringsfeil uten å endre DB eller sende Kafka`() {
        val årstall = 2027
        val kvartal = 2
        val dato = LocalDate.of(2027, 6, 1)
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        val id = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!

        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal)
        // Overstyr sektor slik at aggregert sf_prosent avviker fra LAND (6.2)
        KonsistentTestdata.skrivSektor(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal, tapteDagsverk = "30")

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2027-2:DRY_RUN")

        dvhImportApplikasjon shouldContainLog "DRY_RUN: validering FEILET på steg IMPORT_SEKTOR \\(SF_PROSENT_FEIL\\)".toRegex()

        // Ingen bivirkninger — heller ikke på feil-stien (motsatt av ekte kjøring som ville skrevet lås+steg FEILET)
        lockRepository.hentForPubliseringsdato(id) shouldBe null
        stegRepository.hentAlle(id).shouldBeEmpty()
        publiseringsdatoRepository.hentUprosessertForDato(dato).shouldNotBeNull()
    }

    @Test
    fun `orkestrert import er deterministisk over gjentatte kjøringer med samme data`() {
        val årstall = 2028
        val kvartal = 1
        val dato = LocalDate.of(2028, 3, 1)
        val antallVirksomheter = 1200
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        val id = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!

        KonsistentTestdata.skrivAlleKonsistenteFilerMedVolum(
            gcsContainer = gcsContainer,
            årstall = årstall,
            kvartal = kvartal,
            antallVirksomheter = antallVirksomheter,
        )

        // Kjør samme kvartal 3 ganger med sletting mellom hver kjøring, og fang sluttilstanden per steg.
        val snapshots = (1..3).map {
            kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2028-1")
            ventPåLåsStatus(id, ImportLockStatus.FERDIG, timeoutSekunder = 60)
            val snapshot = stegRepository.hentAlle(id).associate { steg ->
                steg.steg to StegResultat(steg.status, steg.antallRaderLest, steg.antallSendtPaaKafka, steg.sfProsent)
            }
            nullstillOrkestrering(id)
            snapshot
        }

        // Determinisme: alle tre kjøringene gav identisk resultat per steg (status, rader lest, sendt, sf_prosent)
        snapshots[1] shouldBe snapshots[0]
        snapshots[2] shouldBe snapshots[0]

        val referanse = snapshots[0]
        referanse shouldHaveSize 7
        referanse.values.all { it.status == ImportStegStatus.FERDIG } shouldBe true
        referanse.getValue(ImportSteg.IMPORT_VIRKSOMHET).antallRaderLest shouldBe antallVirksomheter
        referanse.getValue(ImportSteg.IMPORT_VIRKSOMHET_METADATA).antallRaderLest shouldBe antallVirksomheter
        referanse.getValue(ImportSteg.IMPORT_LAND).sfProsent.shouldNotBeNull()
    }

    @Test
    fun `virksomheter med ugyldig orgnr filtreres bort og sendes ikke til Kafka`() {
        val årstall = 2029
        val kvartal = 1
        val dato = LocalDate.of(2029, 3, 1)
        val antallGyldige = 100
        // 7-sifrede orgnr = kjent støy i virksomhet.json; skal filtreres av struktur-regexen ^\d{9}$
        val ugyldigeOrgnr = listOf("1234567", "7654321", "9999999", "1112223", "3334445")
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        val id = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!

        val alleOrgnr = KonsistentTestdata.volumOrgnr(antallGyldige) + ugyldigeOrgnr
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal)
        KonsistentTestdata.skrivVirksomhet(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal, orgnr = alleOrgnr)
        KonsistentTestdata.skrivMetadata(gcsContainer = gcsContainer, årstall = årstall, kvartal = kvartal, orgnr = alleOrgnr)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2029-1")
        ventPåLåsStatus(id, ImportLockStatus.FERDIG, timeoutSekunder = 60)

        val steg = stegRepository.hentAlle(id).associateBy { it.steg }
        val virksomhet = steg.getValue(ImportSteg.IMPORT_VIRKSOMHET)
        val metadata = steg.getValue(ImportSteg.IMPORT_VIRKSOMHET_METADATA)

        virksomhet.antallRaderLest shouldBe antallGyldige
        metadata.antallRaderLest shouldBe antallGyldige

        virksomhet.antallSendtPaaKafka shouldBe antallGyldige
        metadata.antallSendtPaaKafka shouldBe antallGyldige
    }

    private data class StegResultat(
        val status: ImportStegStatus,
        val antallRaderLest: Int,
        val antallSendtPaaKafka: Int,
        val sfProsent: BigDecimal?,
    )

    /**
     * Sletter orkestrerings-sporene for et kvartal slik at samme import kan kjøres på nytt fra bunnen
     * (lås = null => taLås). Brukes mellom iterasjonene i determinisme-testen.
     */
    private fun nullstillOrkestrering(publiseringsdatoId: Int) {
        using(sessionOf(testDataSource)) { session ->
            session.run(
                queryOf(
                    "DELETE FROM automatisering_import_steg WHERE publiseringsdato_id = :id",
                    mapOf("id" to publiseringsdatoId),
                ).asUpdate,
            )
            session.run(
                queryOf(
                    "DELETE FROM automatisering_import_lock WHERE publiseringsdato_id = :id",
                    mapOf("id" to publiseringsdatoId),
                ).asUpdate,
            )
            session.run(
                queryOf(
                    "UPDATE publiseringsdato SET prosessert = false WHERE id = :id",
                    mapOf("id" to publiseringsdatoId),
                ).asUpdate,
            )
        }
    }

    @Test
    fun `import feiler når sf_prosent for en kategori avviker fra LAND`() {
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2026, kvartal = 3)
        // Overstyr sektor slik at aggregert sf_prosent (3.0) avviker fra LAND (6.2)
        KonsistentTestdata.skrivSektor(gcsContainer = gcsContainer, årstall = 2026, kvartal = 3, tapteDagsverk = "30")

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2026-3")

        verifiserFeilet(
            årstall = 2026,
            kvartal = 3,
            feilendeSteg = ImportSteg.IMPORT_SEKTOR,
            forventetKontroll = Kontroll.SF_PROSENT_FEIL,
        )
        dvhImportApplikasjon shouldContainLog "avviker fra referanse".toRegex()
    }

    @Test
    fun `import feiler ved feil struktur i næringskode`() {
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2026, kvartal = 1)
        // Næringskode med 10 siffer bryter strukturkravet ^\d{5}$
        KonsistentTestdata.skrivNæringskode(gcsContainer = gcsContainer, årstall = 2026, kvartal = 1, næringskoder = listOf("0111012345"))

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2026-1")

        verifiserFeilet(
            årstall = 2026,
            kvartal = 1,
            feilendeSteg = ImportSteg.IMPORT_NARINGSKODE,
            forventetKontroll = Kontroll.FEIL_STRUKTUR_I_INPUT_FIL,
        )
        dvhImportApplikasjon shouldContainLog "bryter strukturkravet".toRegex()
    }

    @Test
    fun `import feiler når årstall i data ikke matcher jobben`() {
        // Data for 2024, men jobben ber om 2025-4
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2024, kvartal = 4)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2025-4")

        verifiserFeilet(
            årstall = 2025,
            kvartal = 4,
            feilendeSteg = ImportSteg.IMPORT_LAND,
            forventetKontroll = Kontroll.FEIL_ÅRSTALL_ELLER_KVARTAL,
        )
        dvhImportApplikasjon shouldContainLog "feil årstall/kvartal".toRegex()
    }

    /**
     * Verifiserer at orkestreringen endte i riktig feiltilstand:
     *  - låsen er FEILET
     *  - det feilende steget er FEILET med riktig kontroll
     *  - INGEN steg er FERDIG => sendefasen kjørte aldri => ingen Kafka sendt (to-fase-garantien)
     */
    private fun verifiserFeilet(
        årstall: Int,
        kvartal: Int,
        feilendeSteg: ImportSteg,
        forventetKontroll: Kontroll,
    ) {
        val publiseringsdatoId = publiseringsdatoRepository.hentIdForKvartal(årstall, kvartal)!!
        ventPåLåsStatus(publiseringsdatoId, ImportLockStatus.FEILET)

        val steg = stegRepository.hentAlle(publiseringsdatoId)
        val feilet = steg.first { it.steg == feilendeSteg }
        feilet.status shouldBe ImportStegStatus.FEILET
        feilet.kontroll shouldBe forventetKontroll
        steg.none { it.status == ImportStegStatus.FERDIG } shouldBe true
    }

    private fun ventPåLåsStatus(
        publiseringsdatoId: Int,
        status: ImportLockStatus,
        timeoutSekunder: Long = 20,
    ) {
        val slutt = System.currentTimeMillis() + timeoutSekunder * 1000
        while (System.currentTimeMillis() < slutt) {
            if (lockRepository.hentForPubliseringsdato(publiseringsdatoId)?.status == status) return
            Thread.sleep(100)
        }
        throw AssertionError("Lås for publiseringsdato $publiseringsdatoId ble ikke $status innen $timeoutSekunder sek")
    }
}
