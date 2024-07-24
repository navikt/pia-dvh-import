package no.nav.pia.dvhimport.storage

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.importjobb.domene.SykefraværsstatistikkDto
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BucketKlient(
    val gcpStorage: Storage,
    val bucketName: String,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    init {
        ensureBucketExists()
    }

    fun ensureBucketExists() {
        when (gcpStorage.get(bucketName) != null) {
            false -> logger.warn("Bucket $bucketName ikke funnet")
            true -> logger.info("Bucket $bucketName funnet")
        }
    }

    fun ensureBlobExists(path: String, fileName: String): Boolean {
        logger.info("Sjekker at filen '$path/$fileName' finnes i bucket '$bucketName'")

        kotlin.runCatching {
            val blob: Blob = gcpStorage.get(bucketName, "$path/$fileName")
            blob.exists()
        }.onFailure { exception ->
            logger.warn("Fil '$path/$fileName' ble ikke funnet i bucket '$bucketName'. Fikke følgende exception: ", exception)
            return false
        }.onSuccess {
            logger.info("Henter data fra '$path/$fileName' i bucket '$bucketName'")
            return true
        }
        return false
    }

    fun getFromFile(path: String, fileName: String): List<SykefraværsstatistikkDto> {
        if (!ensureBlobExists(path = path, fileName = fileName)) return emptyList()

        val blob: Blob = gcpStorage.get(bucketName, "$path/$fileName")
        val result = blob.getContent().decodeToString()
        return Json.decodeFromString<List<SykefraværsstatistikkDto>>(result)
    }
}