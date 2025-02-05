package no.nav.pia.dvhimport.storage

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.net.MediaType
import io.kotest.matchers.shouldBe
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetSykefraværsstatistikkDto
import no.nav.pia.dvhimport.storage.BucketKlient.Companion.getListFromStream
import no.nav.pia.dvhimport.storage.BucketKlient.Companion.prosesserIBiter
import java.io.File
import java.io.FileInputStream
import java.math.BigDecimal
import kotlin.test.Test

class BucketKlientTest {
    private val storage = LocalStorageHelper.getOptions().service
    private val gjeldendeÅrstallOgKvartal = "2024K2"

    @Test
    fun `skal kunne hente en liste - test med en stor fil`() {
        // In-memory test-bucket fra LocalStorageHelper har begrenset størrelse
        // Derfor tester vi med en Stream fra en lokal fil som ligger på root mappa
        // Filen er generert manuelt med en Python script
        val initialFile = File("virksomhet_5k.json")
        var antallSublist = 0
        var antallVirksomheter = 0

        val fileInputStream = FileInputStream(initialFile)
        val resultList: List<VirksomhetSykefraværsstatistikkDto> =
            getListFromStream(fileInputStream)

        resultList.size shouldBe 5000
        resultList.prosesserIBiter(100) { statistikk ->
            antallSublist += 1
            statistikk.forEach {
                antallVirksomheter += 1
            }
        }
        antallVirksomheter shouldBe 5000
        antallSublist shouldBe 50
    }

    @Test
    fun `skal kunne lese statistikk fra bucket`() {
        val statistikk1 = lagStatistikk("987654321", BigDecimal(10.00), BigDecimal(12.00), BigDecimal(120.00))
        val statistikk2 = lagStatistikk("812345679", BigDecimal(11.00), BigDecimal(11.00), BigDecimal(100.00))
        val statistikk = listOf(statistikk1, statistikk2)
        val statistikkJson = Json.encodeToString(statistikk)
        lagreTestBlobInMemory(
            blobNavn = "$gjeldendeÅrstallOgKvartal/statistikk.json",
            bucketName = "test-in-memory-bucket",
            storage = storage,
            contentType = MediaType.JSON_UTF_8,
            metadata = emptyMap(),
            bytes = statistikkJson.encodeToByteArray(),
        )

        val bucketKlient = BucketKlient(storage, "test-in-memory-bucket")

        val results = bucketKlient.getFromFile(path = gjeldendeÅrstallOgKvartal, fileName = "statistikk.json")
        results shouldBe statistikkJson
    }

    @Test
    fun `returnerer null hvis filen ikke finnes`() {
        lagreTestBlobInMemory(
            blobNavn = "$gjeldendeÅrstallOgKvartal/statistikk.json",
            bucketName = "test-in-memory-bucket",
            storage = storage,
            contentType = MediaType.JSON_UTF_8,
            metadata = emptyMap(),
            bytes = "nothing here".encodeToByteArray(),
        )
        val bucketKlient = BucketKlient(storage, "test-in-memory-bucket")

        val innhold =
            bucketKlient.getFromFile(path = gjeldendeÅrstallOgKvartal, fileName = "denne_filen_finnes_ikke.json")

        innhold shouldBe null
    }

    private fun lagreTestBlobInMemory(
        blobNavn: String,
        bucketName: String,
        storage: Storage,
        contentType: MediaType,
        metadata: Map<String, String>,
        bytes: ByteArray,
    ): Blob {
        val contentTypeVerdi = contentType.toString()
        val blobInfo = BlobInfo.newBuilder(bucketName, blobNavn).setContentType(contentTypeVerdi)
            .setMetadata(metadata + mapOf("content-type" to contentTypeVerdi)).build()

        return storage.create(blobInfo, bytes)
    }

    private fun lagStatistikk(
        orgnr: String,
        prosent: BigDecimal,
        tapteDagsverk: BigDecimal,
        muligeDagsverk: BigDecimal,
    ) = VirksomhetSykefraværsstatistikkDto(
        orgnr = orgnr,
        årstall = 2024,
        kvartal = 3,
        prosent = prosent,
        tapteDagsverk = tapteDagsverk,
        muligeDagsverk = muligeDagsverk,
        tapteDagsverkGradert = BigDecimal(0.00),
        tapteDagsverkPerVarighet = emptyList(),
        antallPersoner = 4,
        rectype = "1",
    )
}
