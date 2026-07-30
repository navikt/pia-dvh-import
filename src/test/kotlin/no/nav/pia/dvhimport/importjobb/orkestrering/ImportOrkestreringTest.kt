package no.nav.pia.dvhimport.importjobb.orkestrering

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.nav.pia.dvhimport.importjobb.ImportService
import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.konfigurasjon.createDataSource
import no.nav.pia.dvhimport.konfigurasjon.runMigration
import no.nav.pia.dvhimport.varsling.SlackVarsler
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.LocalDate
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class ImportOrkestreringTest {

    private val postgres = PostgreSQLContainer("postgres:17-alpine")
    private lateinit var publiseringsdatoRepository: PubliseringsdatoRepository
    private lateinit var lockRepository: ImportLockRepository
    private lateinit var stegRepository: ImportStegRepository
    private lateinit var importService: ImportService
    private lateinit var slackVarsler: SlackVarsler
    private lateinit var orkestrering: ImportOrkestrering

    private val kvartal = ÅrstallOgKvartal(årstall = 2099, kvartal = 1)
    private val dato = LocalDate.of(2099, 5, 28)

    @BeforeTest
    fun setup() {
        postgres.start()
        val dataSource = createDataSource(postgres.jdbcUrl + "&user=${postgres.username}&password=${postgres.password}")
        runMigration(dataSource)
        publiseringsdatoRepository = PubliseringsdatoRepository(dataSource)
        lockRepository = ImportLockRepository(dataSource)
        stegRepository = ImportStegRepository(dataSource)
        importService = mockk()
        slackVarsler = mockk(relaxed = true)
        orkestrering = ImportOrkestrering(importService, lockRepository, stegRepository, publiseringsdatoRepository, slackVarsler)
    }

    @AfterTest
    fun teardown() {
        postgres.stop()
    }

    private fun opprettPubliseringsdato(): Int {
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = kvartal.årstall, kvartal = kvartal.kvartal, dato = dato)
        return publiseringsdatoRepository.hentUprosessertForDato(dato)!!.id
    }

    @Test
    fun `happy path - alle 7 steg FERDIG, lock FERDIG og publiseringsdato prosessert`() {
        val id = opprettPubliseringsdato()
        every { importService.lesOgValiderSteg(any(), any(), any()) } returns StegValideringsresultat(1, null)
        every { importService.sendSteg(any(), any(), any()) } returns 1

        orkestrering.kjørImport(id, kvartal)

        val steg = stegRepository.hentAlle(id)
        steg shouldHaveSize 7
        steg.all { it.status == ImportStegStatus.FERDIG } shouldBe true
        lockRepository.hentForPubliseringsdato(id)!!.status shouldBe ImportLockStatus.FERDIG
        publiseringsdatoRepository.hentUprosessertForDato(dato).shouldBeNull()
    }

    @Test
    fun `validering feiler - ingen sending skjer og lock blir FEILET`() {
        val id = opprettPubliseringsdato()
        every { importService.lesOgValiderSteg(ImportSteg.IMPORT_LAND, any(), any()) } returns StegValideringsresultat(1, null)
        every { importService.lesOgValiderSteg(ImportSteg.IMPORT_SEKTOR, any(), any()) } throws IllegalStateException("valideringsfeil")

        shouldThrow<IllegalStateException> {
            orkestrering.kjørImport(id, kvartal)
        }

        verify(exactly = 0) { importService.sendSteg(any(), any(), any()) }
        lockRepository.hentForPubliseringsdato(id)!!.status shouldBe ImportLockStatus.FEILET
        stegRepository.hent(id, ImportSteg.IMPORT_SEKTOR)!!.status shouldBe ImportStegStatus.FEILET
        stegRepository.hent(id, ImportSteg.IMPORT_LAND)!!.status shouldBe ImportStegStatus.VALIDERT
    }

    @Test
    fun `valideringsfeil gir Slack-varsel med kategori og kontroll`() {
        val id = opprettPubliseringsdato()
        every { importService.lesOgValiderSteg(ImportSteg.IMPORT_LAND, any(), any()) } returns StegValideringsresultat(1, null)
        every { importService.lesOgValiderSteg(ImportSteg.IMPORT_SEKTOR, any(), any()) } throws
            ValideringsfeilException(Kontroll.SF_PROSENT_FEIL, "sykefraværsprosent 5.4 avviker fra referanse 6.2")

        shouldThrow<ValideringsfeilException> {
            orkestrering.kjørImport(id, kvartal)
        }

        verify { slackVarsler.send(match { it.contains("Sektor") && it.contains("SF_PROSENT_FEIL") }) }
    }

    @Test
    fun `gjenopptar fra FEILET steg uten å re-validere allerede validerte steg`() {
        val id = opprettPubliseringsdato()
        every { importService.sendSteg(any(), any(), any()) } returns 1
        every { importService.lesOgValiderSteg(any(), any(), any()) } answers {
            if (firstArg<ImportSteg>() == ImportSteg.IMPORT_NARING) {
                throw IllegalStateException("valideringsfeil")
            } else {
                StegValideringsresultat(1, null)
            }
        }

        shouldThrow<IllegalStateException> {
            orkestrering.kjørImport(id, kvartal)
        }
        lockRepository.hentForPubliseringsdato(id)!!.status shouldBe ImportLockStatus.FEILET

        every { importService.lesOgValiderSteg(any(), any(), any()) } returns StegValideringsresultat(1, null)
        orkestrering.kjørImport(id, kvartal)

        stegRepository.hentAlle(id).all { it.status == ImportStegStatus.FERDIG } shouldBe true
        lockRepository.hentForPubliseringsdato(id)!!.status shouldBe ImportLockStatus.FERDIG
        verify(exactly = 1) { importService.lesOgValiderSteg(ImportSteg.IMPORT_LAND, any(), any()) }
    }

    @Test
    fun `lock hindrer ny kjøring når importen allerede er ferdig`() {
        val id = opprettPubliseringsdato()
        every { importService.lesOgValiderSteg(any(), any(), any()) } returns StegValideringsresultat(1, null)
        every { importService.sendSteg(any(), any(), any()) } returns 1

        orkestrering.kjørImport(id, kvartal)
        orkestrering.kjørImport(id, kvartal)

        verify(exactly = 7) { importService.lesOgValiderSteg(any(), any(), any()) }
        verify(exactly = 7) { importService.sendSteg(any(), any(), any()) }
    }

    @Test
    fun `kjørImportForKvartal feiler når publiseringsdato mangler`() {
        every { importService.lesOgValiderSteg(any(), any(), any()) } returns StegValideringsresultat(1, null)

        shouldThrow<IllegalStateException> {
            orkestrering.kjørImportForKvartal(kvartal, dryRun = false)
        }
    }

    @Test
    fun `kjørImportForPubliseringsdato gjør ingenting når det ikke er publiseringsdato i dag`() {
        orkestrering.kjørImportForPubliseringsdato(dato)

        verify(exactly = 0) { importService.lesOgValiderSteg(any(), any(), any()) }
        verify(exactly = 0) { importService.sendSteg(any(), any(), any()) }
    }

    @Test
    fun `sender Slack-varsler for start, validering, per kategori og ferdig`() {
        val id = opprettPubliseringsdato()
        every { importService.lesOgValiderSteg(any(), any(), any()) } returns StegValideringsresultat(1, null)
        every { importService.sendSteg(any(), any(), any()) } returns 1

        orkestrering.kjørImport(id, kvartal)

        verify { slackVarsler.send(match { it.contains("Import startet") }) }
        verify { slackVarsler.send(match { it.contains("Alle kategorier validert") }) }
        verify { slackVarsler.send(match { it.contains("Land ferdig") }) }
        verify { slackVarsler.send(match { it.contains("Virksomhet metadata ferdig") }) }
        verify { slackVarsler.send(match { it.contains("Import ferdig") }) }
    }

    @Test
    fun `varsler heads-up når det er 3 dager til publiseringsdato`() {
        val iDag = LocalDate.of(2099, 5, 29)
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = 2099, kvartal = 1, dato = iDag.plusDays(3))

        orkestrering.kjørImportForPubliseringsdato(iDag)

        verify { slackVarsler.send(match { it.contains("3 dager til publiseringsdato") }) }
    }

    @Test
    fun `varsler ikke heads-up når antall dager ikke er en milepæl`() {
        val iDag = LocalDate.of(2099, 5, 29)
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = 2099, kvartal = 1, dato = iDag.plusDays(5))

        orkestrering.kjørImportForPubliseringsdato(iDag)

        verify(exactly = 0) { slackVarsler.send(any()) }
    }

    @Test
    fun `dry-run kjører full sti men kaller sendSteg med dryRun`() {
        val id = opprettPubliseringsdato()
        every { importService.lesOgValiderSteg(any(), any(), any()) } returns StegValideringsresultat(1, null)
        every { importService.sendSteg(any(), any(), any()) } returns 1

        orkestrering.kjørImportForKvartal(kvartal, dryRun = true)

        val steg = stegRepository.hentAlle(id)
        steg shouldHaveSize 7
        steg.all { it.status == ImportStegStatus.FERDIG } shouldBe true
        lockRepository.hentForPubliseringsdato(id)!!.status shouldBe ImportLockStatus.FERDIG
        verify(exactly = 7) { importService.sendSteg(any(), any(), true) }
        verify { slackVarsler.send(match { it.contains("dry-run") }) }
    }
}
