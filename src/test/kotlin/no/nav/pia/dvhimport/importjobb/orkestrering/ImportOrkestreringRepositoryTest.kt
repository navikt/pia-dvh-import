package no.nav.pia.dvhimport.importjobb.orkestrering

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoRepository
import no.nav.pia.dvhimport.konfigurasjon.createDataSource
import no.nav.pia.dvhimport.konfigurasjon.runMigration
import org.testcontainers.postgresql.PostgreSQLContainer
import java.math.BigDecimal
import java.time.LocalDate
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class ImportOrkestreringRepositoryTest {

    private val postgres = PostgreSQLContainer("postgres:17-alpine")
    private lateinit var publiseringsdatoRepository: PubliseringsdatoRepository
    private lateinit var lockRepository: ImportLockRepository
    private lateinit var stegRepository: ImportStegRepository

    @BeforeTest
    fun setup() {
        postgres.start()
        val dataSource = createDataSource(postgres.jdbcUrl + "&user=${postgres.username}&password=${postgres.password}")
        runMigration(dataSource)
        publiseringsdatoRepository = PubliseringsdatoRepository(dataSource)
        lockRepository = ImportLockRepository(dataSource)
        stegRepository = ImportStegRepository(dataSource)
    }

    @AfterTest
    fun teardown() {
        postgres.stop()
    }

    private fun opprettPubliseringsdato(
        årstall: Int = 2099,
        kvartal: Int = 1,
        dato: LocalDate = LocalDate.of(2099, 5, 28),
    ): Int {
        publiseringsdatoRepository.lagrePubliseringsdato(årstall = årstall, kvartal = kvartal, dato = dato)
        return publiseringsdatoRepository.hentUprosessertForDato(dato)!!.id
    }

    @Test
    fun `taLås oppretter lås med status STARTET, andre forsøk returnerer null`() {
        val publiseringsdatoId = opprettPubliseringsdato()

        val lås = lockRepository.taLås(publiseringsdatoId)
        lås.shouldNotBeNull()
        lås.status shouldBe ImportLockStatus.STARTET
        lås.publiseringsdatoId shouldBe publiseringsdatoId
        lås.sluttDato.shouldBeNull()

        val andreForsøk = lockRepository.taLås(publiseringsdatoId)
        andreForsøk.shouldBeNull()
    }

    @Test
    fun `markerFerdig setter status FERDIG og slutt_dato`() {
        val publiseringsdatoId = opprettPubliseringsdato()
        lockRepository.taLås(publiseringsdatoId)

        lockRepository.markerFerdig(publiseringsdatoId)

        val lås = lockRepository.hentForPubliseringsdato(publiseringsdatoId)
        lås.shouldNotBeNull()
        lås.status shouldBe ImportLockStatus.FERDIG
        lås.sluttDato.shouldNotBeNull()
    }

    @Test
    fun `markerFeilet og markerStartet endrer lås-status uten slutt_dato`() {
        val publiseringsdatoId = opprettPubliseringsdato()
        lockRepository.taLås(publiseringsdatoId)

        lockRepository.markerFeilet(publiseringsdatoId)
        lockRepository.hentForPubliseringsdato(publiseringsdatoId)!!.status shouldBe ImportLockStatus.FEILET

        lockRepository.markerStartet(publiseringsdatoId)
        val lås = lockRepository.hentForPubliseringsdato(publiseringsdatoId)!!
        lås.status shouldBe ImportLockStatus.STARTET
        lås.sluttDato.shouldBeNull()
    }

    @Test
    fun `opprettStegHvisIkkeFinnes oppretter 7 steg med status PLANLAGT og er idempotent`() {
        val publiseringsdatoId = opprettPubliseringsdato()

        stegRepository.opprettStegHvisIkkeFinnes(publiseringsdatoId)
        stegRepository.opprettStegHvisIkkeFinnes(publiseringsdatoId)

        val steg = stegRepository.hentAlle(publiseringsdatoId)
        steg shouldHaveSize 7
        steg.map { it.steg } shouldBe ImportSteg.iRekkefolge
        steg.all { it.status == ImportStegStatus.PLANLAGT } shouldBe true
        steg.first().rekkefolge shouldBe 1
    }

    @Test
    fun `steg-livssyklus PLANLAGT til VALIDERT til FERDIG`() {
        val publiseringsdatoId = opprettPubliseringsdato()
        stegRepository.opprettStegHvisIkkeFinnes(publiseringsdatoId)

        stegRepository.markerStartet(publiseringsdatoId, ImportSteg.IMPORT_LAND)
        stegRepository.hent(publiseringsdatoId, ImportSteg.IMPORT_LAND)!!.status shouldBe ImportStegStatus.STARTET

        stegRepository.markerValidert(
            publiseringsdatoId = publiseringsdatoId,
            steg = ImportSteg.IMPORT_LAND,
            antallRaderLest = 1,
            sfProsent = BigDecimal("6.10"),
        )
        val validert = stegRepository.hent(publiseringsdatoId, ImportSteg.IMPORT_LAND)!!
        validert.status shouldBe ImportStegStatus.VALIDERT
        validert.antallRaderLest shouldBe 1
        validert.sfProsent shouldBe BigDecimal("6.10")

        stegRepository.markerFerdig(publiseringsdatoId, ImportSteg.IMPORT_LAND, antallSendtPaaKafka = 1)
        val ferdig = stegRepository.hent(publiseringsdatoId, ImportSteg.IMPORT_LAND)!!
        ferdig.status shouldBe ImportStegStatus.FERDIG
        ferdig.kontroll shouldBe Kontroll.OK
        ferdig.antallSendtPaaKafka shouldBe 1
        ferdig.sluttDato.shouldNotBeNull()
    }

    @Test
    fun `markerFeilet setter status FEILET med riktig kontroll`() {
        val publiseringsdatoId = opprettPubliseringsdato()
        stegRepository.opprettStegHvisIkkeFinnes(publiseringsdatoId)

        stegRepository.markerFeilet(
            publiseringsdatoId = publiseringsdatoId,
            steg = ImportSteg.IMPORT_NARINGSKODE,
            kontroll = Kontroll.FEIL_STRUKTUR_I_INPUT_FIL,
        )

        val steg = stegRepository.hent(publiseringsdatoId, ImportSteg.IMPORT_NARINGSKODE)!!
        steg.status shouldBe ImportStegStatus.FEILET
        steg.kontroll shouldBe Kontroll.FEIL_STRUKTUR_I_INPUT_FIL
        steg.sluttDato.shouldNotBeNull()
    }
}
