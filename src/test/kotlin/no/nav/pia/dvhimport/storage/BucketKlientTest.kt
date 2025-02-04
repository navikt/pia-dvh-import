package no.nav.pia.dvhimport.storage

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.net.MediaType
import io.kotest.matchers.ints.shouldBeLessThanOrEqual
import io.kotest.matchers.shouldBe
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetSykefraværsstatistikkDto
import no.nav.pia.dvhimport.storage.BucketKlient.Companion.getSequenceFromStream
import no.nav.pia.dvhimport.storage.BucketKlient.Companion.prosesserIBiter
import java.io.ByteArrayInputStream
import java.io.File
import java.math.BigDecimal
import kotlin.test.Test

class BucketKlientTest {
    private val storage = LocalStorageHelper.getOptions().service
    private val gjeldendeÅrstallOgKvartal = "2024K2"

    @Test
    fun `skal kunne lese statistikk fra bucket i Sequence`() {
        val statistikk = mutableListOf<VirksomhetSykefraværsstatistikkDto>()
        for (i in 1..86) {
            val orgnrAsInt = 900000000 + i
            val virksomhetStatistikk =
                lagStatistikk(orgnrAsInt.toString(), BigDecimal(10.00), BigDecimal(12.00), BigDecimal(120.00))
            statistikk.add(virksomhetStatistikk)
        }

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

        val resultAsSequence = bucketKlient.getFromHugeFileAsSequence<VirksomhetSykefraværsstatistikkDto>(
            path = gjeldendeÅrstallOgKvartal,
            fileName = "statistikk.json",
        )

        var antallVirksomheter = 0
        resultAsSequence.prosesserIBiter(størrelse = 50) { items ->
            items.size shouldBeLessThanOrEqual 50
            println("Size of processed list: ${items.size}")
            antallVirksomheter += items.size
        }
        antallVirksomheter shouldBe 86
    }

    @Test
    fun `skal kunne prosessere en sequence i flere biter - test med en stor fil`() {
        // In-memory test-bucket fra LocalStorageHelper har begrenset størrelse
        // Derfor tester vi med en Stream fra en lokal fil som ligger på root mappa
        // Filen er generert manuelt med en Python script
        val initialFile = File("virksomhet_5k.json")
        initialFile.readLines().map { it.encodeToByteArray() }
        val ins = ByteArrayInputStream(initialFile.readBytes())
        val sequence: Sequence<VirksomhetSykefraværsstatistikkDto> = getSequenceFromStream(ins)

        var antallSequense = 0
        var antallVirksomheter = 0
        sequence.prosesserIBiter(størrelse = 2) { items ->
            items.size shouldBeLessThanOrEqual 2
            antallSequense += 1
            // println("Size of processed list: ${items.size}")
            items.forEach {
                antallVirksomheter += 1
                it.orgnr.length shouldBe 9
            }
        }
        antallSequense shouldBe 2500
        antallVirksomheter shouldBe 5000
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
