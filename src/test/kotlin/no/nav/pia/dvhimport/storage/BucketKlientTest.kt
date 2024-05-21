package no.nav.pia.dvhimport.storage

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.net.MediaType
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.importjobb.domene.SykefraværsstatistikkDto
import kotlin.test.Test


class BucketKlientTest {
    private val storage = LocalStorageHelper.getOptions().service

    @Test
    fun `skal kunne lese statistikk fra bucket`() {
        val statistikk1 = SykefraværsstatistikkDto(
            orgnr = "987654321",
            årstall = 2024,
            kvartal = 3,
            tapteDagsverk = 12,
            muligeDagsverk = 100,
            antallPersoner = 4
        )
        val statistikk2 = SykefraværsstatistikkDto(
            orgnr = "321456789",
            årstall = 2024,
            kvartal = 3,
            tapteDagsverk = 120,
            muligeDagsverk = 1000,
            antallPersoner = 40
        )
        val statistikk = listOf(statistikk1, statistikk2)
        lagreTestBlobInMemory(
            blobNavn = "statistikk.json",
            bucketName = "test-in-memory-bucket",
            storage = storage,
            contentType = MediaType.JSON_UTF_8,
            metadata = emptyMap(),
            bytes = Json.encodeToString(statistikk).encodeToByteArray()
        )

        val bucketKlient = BucketKlient(storage, "test-in-memory-bucket")
        val results = bucketKlient.getFromFile("statistikk.json")

        results.size shouldBe 2
        results shouldContainExactlyInAnyOrder statistikk
    }
}

fun lagreTestBlobInMemory(
    blobNavn: String,
    bucketName: String,
    storage: Storage,
    contentType: MediaType,
    metadata: Map<String, String>,
    bytes: ByteArray,
): Blob {
    val contentTypeVerdi = contentType.toString()
    val blobInfo =
        BlobInfo.newBuilder(bucketName, blobNavn)
            .setContentType(contentTypeVerdi)
            .setMetadata(metadata + mapOf("content-type" to contentTypeVerdi))
            .build()

    return storage.create(blobInfo, bytes)
}
