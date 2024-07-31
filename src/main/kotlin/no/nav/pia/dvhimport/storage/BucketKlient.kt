package no.nav.pia.dvhimport.storage

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import no.nav.pia.dvhimport.importjobb.Feil

class BucketKlient(
    val gcpStorage: Storage,
    val bucketName: String,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    init {
        logger.info("BucketKlient for bucket '$bucketName' er klar")
    }

    fun ensureBucketExists() {
        val bucket = gcpStorage.get(bucketName)

        when (bucket != null) {
            false -> logger.warn("Bucket $bucketName ikke funnet")
            true -> logger.info("Bucket $bucketName funnet")
        }
    }

    fun ensureFileExists(path: String, fileName: String): Boolean {

        logger.info("Sjekker at filen '$fileName', i path '$path' finnes i bucket '$bucketName'")

        kotlin.runCatching {
            val blob: Blob = gcpStorage.get(bucketName, fileName)
            blob.exists()
        }.onFailure { exception ->
            logger.warn("Fil '$fileName' ble ikke funnet i bucket '$bucketName'. Fikke følgende exception: ", exception)
            return false
        }.onSuccess {
            logger.info("Henter data fra '$fileName' i bucket '$bucketName'")
            return true
        }
        return false
    }

    fun getFromFile(path: String, fileName: String): String {
        val fil = if (path.isNotEmpty()) "$path/$fileName" else fileName
        logger.info("Fetch data i bucket '$bucketName' fra fil i path '$path' med filnavn '$fileName'")

        val blob: Blob = try {
            gcpStorage.get(bucketName, fil)
        } catch (npe: NullPointerException) {
            throw Feil(
                feilmelding = "Finner ikke fil '$fil' fra bucket '$bucketName'",
                opprinneligException = npe
            )
        }
        val result = blob.getContent().decodeToString()
        return result
    }
}