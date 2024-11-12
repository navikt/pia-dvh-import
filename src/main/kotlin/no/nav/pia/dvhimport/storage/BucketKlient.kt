package no.nav.pia.dvhimport.storage

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BucketKlient(
    val gcpStorage: Storage,
    val bucketName: String,
) {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    init {
        logger.info("BucketKlient for bucket '$bucketName' er klar")
    }

    fun sjekkBucketExists(): Boolean {
        val bucket = gcpStorage.get(bucketName)
        val erBucketFunnet = bucket != null

        when (erBucketFunnet) {
            false -> logger.warn("Bucket $bucketName ikke funnet")
            true -> logger.info("Bucket $bucketName funnet")
        }
        return erBucketFunnet
    }

    fun ensureFileExists(
        path: String,
        fileName: String,
    ): Boolean {
        val fil = if (path.isNotEmpty()) "$path/$fileName" else fileName
        logger.info("Sjekker at filen '$fileName', i path '$path' finnes i bucket '$bucketName' (søk på fil: '$fil')")

        kotlin.runCatching {
            val blob: Blob = gcpStorage.get(bucketName, fil)
            blob.exists()
        }.onFailure { exception ->
            logger.warn("Fil '$fil' ble ikke funnet i bucket '$bucketName'. Fikke følgende exception: ", exception)
            return false
        }.onSuccess {
            logger.info("Henter data fra '$fileName' i bucket '$bucketName'")
            return true
        }
        return false
    }

    fun getFromFile(
        path: String,
        fileName: String,
    ): String? {
        val fil = if (path.isNotEmpty()) "$path/$fileName" else fileName
        logger.info("Fetch data i bucket '$bucketName' fra fil i path '$path' med filnavn '$fileName'")

        val blob: Blob? = try {
            gcpStorage.get(bucketName, fil)
        } catch (npe: NullPointerException) {
            logger.error("Finner ikke fil '$fil' fra bucket '$bucketName'")
            null
        }
        return blob?.getContent()?.decodeToString()
    }
}
