package no.nav.pia.dvhimport.importjobb

import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
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
        val resultat = repository.hentUprosessertForDato(dato)
        resultat.shouldNotBeNull()
        resultat.årstall shouldBe 2025
        resultat.kvartal shouldBe 1
        resultat.dato shouldBe dato
        resultat.prosessert shouldBe false
    }

    @Test
    fun `lagrePubliseringsdato oppdaterer eksisterende rad og resetter prosessert`() {
        val gammeltDato = LocalDate.of(2025, 9, 1)
        val nyttDato = LocalDate.of(2025, 9, 4)

        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 2, dato = gammeltDato)
        repository.markerSomProsessert(repository.hentUprosessertForDato(gammeltDato)!!.id)

        val endret = repository.lagrePubliseringsdato(årstall = 2025, kvartal = 2, dato = nyttDato)

        endret shouldBe LagreResultat.OPPDATERT
        val resultat = repository.hentUprosessertForDato(nyttDato)
        resultat.shouldNotBeNull()
        resultat.dato shouldBe nyttDato
        resultat.prosessert shouldBe false

        repository.hentUprosessertForDato(gammeltDato).shouldBeNull()
    }

    @Test
    fun `lagrePubliseringsdato med samme dato returnerer false og endrer ingenting`() {
        val dato = LocalDate.of(2025, 11, 28)
        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 3, dato = dato)

        val endret = repository.lagrePubliseringsdato(årstall = 2025, kvartal = 3, dato = dato)

        endret shouldBe LagreResultat.UENDRET
        val resultat = repository.hentUprosessertForDato(dato)
        resultat.shouldNotBeNull()
    }

    @Test
    fun `markerer som prosessert`() {
        val dato = LocalDate.of(2025, 12, 4)
        repository.lagrePubliseringsdato(årstall = 2025, kvartal = 3, dato = dato)

        val uprosessert = repository.hentUprosessertForDato(dato)
        uprosessert.shouldNotBeNull()

        repository.markerSomProsessert(uprosessert.id)

        repository.hentUprosessertForDato(dato).shouldBeNull()
    }

    @Test
    fun `returnerer null når ingen matcher dato`() {
        val resultat = repository.hentUprosessertForDato(LocalDate.of(2099, 1, 1))
        resultat.shouldBeNull()
    }

    @Test
    fun `returnerer ikke allerede prosesserte rader`() {
        val dato = LocalDate.of(2026, 3, 5)
        repository.lagrePubliseringsdato(årstall = 2026, kvartal = 4, dato = dato)

        val rad = repository.hentUprosessertForDato(dato)
        repository.markerSomProsessert(rad!!.id)

        repository.hentUprosessertForDato(dato).shouldBeNull()
    }
}
