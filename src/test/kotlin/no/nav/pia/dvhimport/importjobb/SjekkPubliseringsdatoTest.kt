package no.nav.pia.dvhimport.importjobb

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.Runs
import io.mockk.spyk
import io.mockk.verify
import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.konfigurasjon.createDataSource
import no.nav.pia.dvhimport.konfigurasjon.runMigration
import no.nav.pia.dvhimport.storage.BucketKlient
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.LocalDate
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class SjekkPubliseringsdatoTest {

    private val postgres = PostgreSQLContainer("postgres:17-alpine")
    private lateinit var repository: PubliseringsdatoRepository
    private lateinit var importService: ImportService

    @BeforeTest
    fun setup() {
        postgres.start()
        val dataSource = createDataSource(postgres.jdbcUrl + "&user=${postgres.username}&password=${postgres.password}")
        runMigration(dataSource)
        repository = PubliseringsdatoRepository(dataSource)
        importService = spyk(
            ImportService(
                bucketKlient = mockk<BucketKlient>(),
                brukÅrOgKvartalIPathTilFilene = false,
                publiseringsdatoRepository = repository,
            ),
        )
        every { importService.importAlleStatistikkKategorier(any(), any()) } just Runs
    }

    @AfterTest
    fun teardown() {
        postgres.stop()
    }

    @Test
    fun `ingen import når det ikke er publiseringsdato i dag`() {
        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 1, dato = LocalDate.of(2099, 1, 1))

        importService.sjekkPubliseringsdatoOgStartImport(dato = LocalDate.now())

        verify(exactly = 0) { importService.importAlleStatistikkKategorier(any(), any()) }
    }

    @Test
    fun `starter import og markerer som prosessert når det er publiseringsdato i dag`() {
        val iDag = LocalDate.now()

        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 1, dato = iDag)

        importService.sjekkPubliseringsdatoOgStartImport(dato = iDag)

        verify(exactly = 1) { importService.importAlleStatistikkKategorier(ÅrstallOgKvartal(årstall = 2025, kvartal = 1), null) }
        repository.hentUprosesserteForDato(iDag) shouldHaveSize 0
    }

    @Test
    fun `hopper over allerede prosesserte rader`() {
        val iDag = LocalDate.now()
        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 2, dato = iDag)
        val rader = repository.hentUprosesserteForDato(iDag)
        repository.markerSomProsessert(rader.first().id)

        importService.sjekkPubliseringsdatoOgStartImport(dato = iDag)

        verify(exactly = 0) { importService.importAlleStatistikkKategorier(any(), any()) }
    }

    @Test
    fun `prosesserer flere publiseringsdatoer på samme dag`() {
        val iDag = LocalDate.now()

        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 3, dato = iDag)
        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 4, dato = iDag)

        importService.sjekkPubliseringsdatoOgStartImport(dato = iDag)

        verify(exactly = 1) { importService.importAlleStatistikkKategorier(ÅrstallOgKvartal(årstall = 2025, kvartal = 3), null) }
        verify(exactly = 1) { importService.importAlleStatistikkKategorier(ÅrstallOgKvartal(årstall = 2025, kvartal = 4), null) }
        repository.hentUprosesserteForDato(iDag) shouldHaveSize 0
    }

    @Test
    fun `markerer ikke som prosessert hvis import feiler`() {
        val iDag = LocalDate.now()
        every { importService.importAlleStatistikkKategorier(any(), any()) } throws IllegalStateException("GCS feil")

        repository.lagrePubliseringsdato(årstall = 2026, kvartal = 1, dato = iDag)

        try {
            importService.sjekkPubliseringsdatoOgStartImport(dato = iDag)
        } catch (_: IllegalStateException) {
        }

        repository.hentUprosesserteForDato(iDag) shouldHaveSize 1
        repository.hentUprosesserteForDato(iDag).first().prosessert shouldBe false
    }
}
