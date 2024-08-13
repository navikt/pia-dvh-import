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
import java.math.BigDecimal
import kotlin.test.Test


class BucketKlientTest {
    private val storage = LocalStorageHelper.getOptions().service

    @Test
    fun `skal kunne lese statistikk fra bucket`() {
        val statistikk1 = lagStatistikk("987654321", BigDecimal(10.00), BigDecimal(12.00), BigDecimal(120.00))
        val statistikk2 = lagStatistikk("812345679", BigDecimal(11.00), BigDecimal(11.00), BigDecimal(100.00))
        val statistikk = listOf(statistikk1, statistikk2)
        val statistikkJson = Json.encodeToString(statistikk)
        lagreTestBlobInMemory(
            blobNavn = "2024K1/statistikk.json",
            bucketName = "test-in-memory-bucket",
            storage = storage,
            contentType = MediaType.JSON_UTF_8,
            metadata = emptyMap(),
            bytes = statistikkJson.encodeToByteArray()
        )

        val bucketKlient = BucketKlient(storage, "test-in-memory-bucket")

        val results = bucketKlient.getFromFile(path = "2024K1", fileName = "statistikk.json")
        results shouldBe statistikkJson
    }

    @Test
    fun `returnerer null hvis filen ikke finnes`() {
        lagreTestBlobInMemory(
            blobNavn = "2024K1/statistikk.json",
            bucketName = "test-in-memory-bucket",
            storage = storage,
            contentType = MediaType.JSON_UTF_8,
            metadata = emptyMap(),
            bytes = "nothing here".encodeToByteArray()
        )
        val bucketKlient = BucketKlient(storage, "test-in-memory-bucket")

        val innhold = bucketKlient.getFromFile(path = "2024K1", fileName = "statistikk_not_found.json")

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
        muligeDagsverk: BigDecimal
    ) = VirksomhetSykefraværsstatistikkDto(
        orgnr = orgnr,
        årstall = 2024,
        kvartal = 3,
        prosent = prosent,
        tapteDagsverk = tapteDagsverk,
        muligeDagsverk = muligeDagsverk,
        tapteDagsverkGradert = BigDecimal(0.00),
        tapteDagsverkPerVarighet = emptyList(),
        antallPersoner = BigDecimal(4),
        rectype = "1",
    )
}