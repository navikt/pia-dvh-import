package no.nav.pia.dvhimport.importjobb

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.publiseringsdato.LagreResultat
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.konfigurasjon.createDataSource
import no.nav.pia.dvhimport.konfigurasjon.runMigration
import org.testcontainers.postgresql.PostgreSQLContainer
import java.time.LocalDate
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class PubliseringsdatoRepositoryTest {

    private val postgres = PostgreSQLContainer("postgres:17-alpine")
    private lateinit var repository: PubliseringsdatoRepository

    @BeforeTest
    fun setup() {
        postgres.start()
        val dataSource = createDataSource(postgres.jdbcUrl + "&user=${postgres.username}&password=${postgres.password}")
        runMigration(dataSource)
        repository = PubliseringsdatoRepository(dataSource)
    }

    @AfterTest
    fun teardown() {
        postgres.stop()
    }

    @Test
    fun `lagrePubliseringsdato lagrer og henter`() {
        val dato = LocalDate.of(2025, 6, 5)
        val lagreResultat = repository.lagrePubliseringsdato(årstall = 2025, kvartal = 1, dato = dato)

        lagreResultat shouldBe LagreResultat.NY
        val resultat = repository.hentUprosesserteForDato(dato)
        resultat shouldHaveSize 1
        resultat.first().årstall shouldBe 2025
        resultat.first().kvartal shouldBe 1
        resultat.first().dato shouldBe dato
        resultat.first().prosessert shouldBe false
    }

    @Test
    fun `lagrePubliseringsdato oppdaterer eksisterende rad og resetter prosessert`() {
        val gammeltDato = LocalDate.of(2025, 9, 1)
        val nyttDato = LocalDate.of(2025, 9, 4)

        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 2, dato = gammeltDato)
        repository.markerSomProsessert(repository.hentUprosesserteForDato(gammeltDato).first().id)

        val endret = repository.lagrePubliseringsdato(årstall = 2025, kvartal = 2, dato = nyttDato)

        endret shouldBe LagreResultat.OPPDATERT
        val resultat = repository.hentUprosesserteForDato(nyttDato)
        resultat shouldHaveSize 1
        resultat.first().dato shouldBe nyttDato
        resultat.first().prosessert shouldBe false

        repository.hentUprosesserteForDato(gammeltDato) shouldHaveSize 0
    }

    @Test
    fun `lagrePubliseringsdato med samme dato returnerer false og endrer ingenting`() {
        val dato = LocalDate.of(2025, 11, 28)
        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 3, dato = dato)

        val endret = repository.lagrePubliseringsdato(årstall = 2025, kvartal = 3, dato = dato)

        endret shouldBe LagreResultat.UENDRET
        val resultat = repository.hentUprosesserteForDato(dato)
        resultat shouldHaveSize 1
    }

    @Test
    fun `markerer som prosessert`() {
        val dato = LocalDate.of(2025, 12, 4)
        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 3, dato = dato)

        val uprosesserte = repository.hentUprosesserteForDato(dato)
        uprosesserte shouldHaveSize 1

        repository.markerSomProsessert(uprosesserte.first().id)

        repository.hentUprosesserteForDato(dato) shouldHaveSize 0
    }

    @Test
    fun `returnerer tom liste når ingen matcher dato`() {
        val resultat = repository.hentUprosesserteForDato(LocalDate.of(2099, 1, 1))
        resultat shouldHaveSize 0
    }

    @Test
    fun `returnerer ikke allerede prosesserte rader`() {
        val dato = LocalDate.of(2026, 3, 5)
        repository.lagrePubliseringsdato(årstall = 2026, kvartal = 4, dato = dato)

        val rader = repository.hentUprosesserteForDato(dato)
        repository.markerSomProsessert(rader.first().id)

        repository.hentUprosesserteForDato(dato) shouldHaveSize 0
    }
}
