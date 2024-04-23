package no.nav.pia.dvhimport.storage

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
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


    fun ensureBucketExists(bucketName: String) {
        when (gcpStorage.get(bucketName) != null) {
            false -> throw IllegalStateException("Fant ikke bucket med navn $bucketName")
            true -> logger.info("Bucket $bucketName funnet")
        }
    }

    fun ensureBlobIdExists(fileName: String): Boolean {
        val storageKey = fileName
        val blob: Blob = gcpStorage.get(BlobId.of(bucketName, storageKey))
        kotlin.runCatching {
            blob.exists()
        }.onFailure {
            // do nothing
        }.onSuccess {
            logger.info("Henter data for $storageKey fra $bucketName")
            return true
        }
        logger.info("Data for $storageKey finnes ikke for $bucketName")
        return false
    }


    fun getFromFile(filnavn: String): List<SykefraværsstatistikkDto> {
        val blob: Blob = gcpStorage.get(bucketName, filnavn)
        val result = blob.getContent().decodeToString()
        return Json.decodeFromString<List<SykefraværsstatistikkDto>>(result)
    }


}