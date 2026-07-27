package no.nav.pia.dvhimport.helper

import ia.felles.definisjoner.bransjer.Bransje
import ia.felles.definisjoner.bransjer.BransjeId
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.tilFilNavn
import no.nav.pia.dvhimport.importjobb.domene.DvhMetadata
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori

/**
 * Objekt-basert, konsistent testdata for hele orkestreringen.
 * Hver kategori aggregerer til samme sykefraværsprosent (62 / 1000 = 6.2 %), slik at
 * sf_prosent-likhetssjekken mot LAND passerer. Hver skrive-funksjon kan overstyres for å
 * lage målrettet korrupsjon i negative tester.
 */
object KonsistentTestdata {
    private const val TAPTE = "62"
    private const val MULIGE = "1000"
    private const val PROSENT = "6.2"

    // Ekte barnehage-næringskode (5 siffer) slik at BARNEHAGER-bransjen faktisk utledes
    // og bransje-steget ikke ender med tom liste (0/0 i kalkulering).
    private val barnehageNæringskode = (Bransje.BARNEHAGER.bransjeId as BransjeId.Næringskoder).næringskoder.first()

    fun skrivAlleKonsistenteFiler(
        gcsContainer: GoogleCloudStorageContainerHelper,
        årstall: Int = 2026,
        kvartal: Int = 2,
    ) {
        skrivLand(gcsContainer, årstall, kvartal)
        skrivSektor(gcsContainer, årstall, kvartal)
        skrivNæring(gcsContainer, årstall, kvartal)
        skrivNæringskode(gcsContainer, årstall, kvartal)
        skrivVirksomhet(gcsContainer, årstall, kvartal)
        skrivMetadata(gcsContainer, årstall, kvartal)
    }

    fun skrivLand(
        gcsContainer: GoogleCloudStorageContainerHelper,
        årstall: Int = 2026,
        kvartal: Int = 2,
        prosent: String = PROSENT,
        tapteDagsverk: String = TAPTE,
        muligeDagsverk: String = MULIGE,
    ) = skriv(
        gcsContainer,
        StatistikkKategori.LAND,
        """
        [{"årstall": $årstall, "kvartal": $kvartal, "land": "NO", "prosent": "$prosent",
          "tapteDagsverk": "$tapteDagsverk", "muligeDagsverk": "$muligeDagsverk", "antallPersoner": "100"}]
        """.trimIndent(),
    )

    fun skrivSektor(
        gcsContainer: GoogleCloudStorageContainerHelper,
        årstall: Int = 2026,
        kvartal: Int = 2,
        tapteDagsverk: String = TAPTE,
        muligeDagsverk: String = MULIGE,
    ) = skriv(
        gcsContainer,
        StatistikkKategori.SEKTOR,
        listOf("1", "2", "3").joinToString(prefix = "[", postfix = "]", separator = ",") { sektor ->
            statistikkRad("sektor", sektor, årstall, kvartal, tapteDagsverk, muligeDagsverk)
        },
    )

    fun skrivNæring(
        gcsContainer: GoogleCloudStorageContainerHelper,
        årstall: Int = 2026,
        kvartal: Int = 2,
        næringer: List<String> = listOf("01", "02"),
    ) = skriv(
        gcsContainer,
        StatistikkKategori.NÆRING,
        næringer.joinToString(prefix = "[", postfix = "]", separator = ",") { næring ->
            statistikkRadMedVarighet("næring", næring, årstall, kvartal)
        },
    )

    fun skrivNæringskode(
        gcsContainer: GoogleCloudStorageContainerHelper,
        årstall: Int = 2026,
        kvartal: Int = 2,
        næringskoder: List<String> = listOf(barnehageNæringskode),
    ) = skriv(
        gcsContainer,
        StatistikkKategori.NÆRINGSKODE,
        næringskoder.joinToString(prefix = "[", postfix = "]", separator = ",") { kode ->
            statistikkRadMedVarighet("næringskode", kode, årstall, kvartal)
        },
    )

    fun skrivVirksomhet(
        gcsContainer: GoogleCloudStorageContainerHelper,
        årstall: Int = 2026,
        kvartal: Int = 2,
        orgnr: List<String> = listOf("987654321", "987654322"),
        rectype: String = "2",
    ) = skriv(
        gcsContainer,
        StatistikkKategori.VIRKSOMHET,
        orgnr.joinToString(prefix = "[", postfix = "]", separator = ",") { org ->
            """
            {"årstall": $årstall, "kvartal": $kvartal, "orgnr": "$org", "prosent": "$PROSENT",
             "tapteDagsverk": "$TAPTE", "muligeDagsverk": "$MULIGE", "tapteDagsverkGradert": 5,
             "tapteDagsverkPerVarighet": [{"varighet": "D", "tapteDagsverk": 10}],
             "antallPersoner": "20", "rectype": "$rectype"}
            """.trimIndent()
        },
    )

    fun skrivMetadata(
        gcsContainer: GoogleCloudStorageContainerHelper,
        årstall: Int = 2026,
        kvartal: Int = 2,
        orgnr: List<String> = listOf("987654321", "987654322"),
    ) {
        val filnavn = tilFilNavn(DvhMetadata.VIRKSOMHET_METADATA)
        val json = orgnr.joinToString(prefix = "[", postfix = "]", separator = ",") { org ->
            """
            {"årstall": $årstall, "kvartal": $kvartal, "orgnr": "$org", "sektor": "2",
             "primærnæring": "01", "primærnæringskode": "01110", "rectype": "2"}
            """.trimIndent()
        }
        gcsContainer.lagreTestBlob(blobNavn = filnavn, bytes = json.encodeToByteArray())
        gcsContainer.verifiserBlobFinnes(blobNavn = filnavn) shouldBe true
    }

    /**
     * Skriver et realistisk volum av virksomheter + tilhørende metadata (streaming-stien),
     * med de fire små kategoriene uendret. Hver virksomhet har samme tapte/mulige (62/1000),
     * så aggregert sf_prosent forblir 6.2 % uansett antall. Brukes til determinisme-/volumtest.
     */
    fun skrivAlleKonsistenteFilerMedVolum(
        gcsContainer: GoogleCloudStorageContainerHelper,
        årstall: Int = 2026,
        kvartal: Int = 2,
        antallVirksomheter: Int = 1200,
    ) {
        skrivLand(gcsContainer, årstall, kvartal)
        skrivSektor(gcsContainer, årstall, kvartal)
        skrivNæring(gcsContainer, årstall, kvartal)
        skrivNæringskode(gcsContainer, årstall, kvartal)
        skrivVirksomhet(gcsContainer, årstall, kvartal, orgnr = volumOrgnr(antallVirksomheter))
        skrivMetadata(gcsContainer, årstall, kvartal, orgnr = volumOrgnr(antallVirksomheter))
    }

    // Unike, 9-sifrede orgnr (100000000, 100000001, ...) — passerer struktur-regexen ^\d{9}$.
    fun volumOrgnr(antall: Int): List<String> = (0 until antall).map { (100_000_000 + it).toString() }

    private fun statistikkRad(
        felt: String,
        verdi: String,
        årstall: Int,
        kvartal: Int,
        tapteDagsverk: String,
        muligeDagsverk: String,
    ) = """
        {"årstall": $årstall, "kvartal": $kvartal, "$felt": "$verdi", "prosent": "$PROSENT",
         "tapteDagsverk": "$tapteDagsverk", "muligeDagsverk": "$muligeDagsverk", "antallPersoner": "100"}
    """.trimIndent()

    private fun statistikkRadMedVarighet(
        felt: String,
        verdi: String,
        årstall: Int,
        kvartal: Int,
    ) = """
        {"årstall": $årstall, "kvartal": $kvartal, "$felt": "$verdi", "prosent": "$PROSENT",
         "tapteDagsverk": "$TAPTE", "muligeDagsverk": "$MULIGE", "tapteDagsverkGradert": 5,
         "tapteDagsverkPerVarighet": [{"varighet": "D", "tapteDagsverk": 10}], "antallPersoner": "100"}
    """.trimIndent()

    private fun skriv(
        gcsContainer: GoogleCloudStorageContainerHelper,
        kategori: StatistikkKategori,
        json: String,
    ) {
        val filnavn = tilFilNavn(kategori)
        gcsContainer.lagreTestBlob(blobNavn = filnavn, bytes = json.encodeToByteArray())
        gcsContainer.verifiserBlobFinnes(blobNavn = filnavn) shouldBe true
    }
}
