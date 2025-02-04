package no.nav.pia.dvhimport.storage

import com.google.cloud.ReadChannel
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.DecodeSequenceMode
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeToSequence
import no.nav.pia.dvhimport.importjobb.domene.Sykefraværsstatistikk
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.nio.channels.Channels

class BucketKlient(
    val gcpStorage: Storage,
    val bucketName: String,
) {
    val logger: Logger = LoggerFactory.getLogger(this::class.java)

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
        val blob = getBlob(this, path = path, fileName = fileName)
        return blob?.getContent()?.decodeToString()
    }

    inline fun <reified T : Sykefraværsstatistikk> getFromHugeFileAsSequence(
        path: String,
        fileName: String,
    ): Sequence<T> {
        val blob = getBlob(this, path = path, fileName = fileName)
        val readChannel: ReadChannel? = blob?.reader()

        if (readChannel == null || !readChannel.isOpen) {
            logger.info("Channel '$readChannel' is empty")
            return emptySequence()
        }

        val inputStream: InputStream? = readChannel.let { Channels.newInputStream(it) }

        return getSequenceFromStream<T>(inputStream)
    }

    companion object {
        fun getBlob(
            bucketKlient: BucketKlient,
            path: String,
            fileName: String,
        ): Blob? {
            val fil = if (path.isNotEmpty()) "$path/$fileName" else fileName
            bucketKlient.logger.info("Fetch data i bucket '${bucketKlient.bucketName}' fra fil i path '$path' med filnavn '$fileName'")

            val blob: Blob? = try {
                bucketKlient.gcpStorage.get(bucketKlient.bucketName, fil)
            } catch (npe: NullPointerException) {
                bucketKlient.logger.error("Finner ikke fil '$fil' fra bucket '${bucketKlient.bucketName}'")
                null
            }
            return blob
        }

        @OptIn(ExperimentalSerializationApi::class)
        inline fun <reified T : Sykefraværsstatistikk> getSequenceFromStream(inputStream: InputStream?): Sequence<T> {
            val jsonParser = Json { ignoreUnknownKeys = true }
            return inputStream?.use {
                jsonParser.decodeToSequence<T>(
                    stream = it,
                    format = DecodeSequenceMode.ARRAY_WRAPPED,
                )
            } ?: emptySequence()
        }

        fun <T> Sequence<T>?.prosesserIBiter(
            størrelse: Int,
            block: (items: List<T>) -> Unit,
        ) {
            if (this == null) {
                return
            }
            this.delIBiter(størrelse = størrelse).forEach { sequence ->
                block(sequence.toList())
            }
        }

        private fun <T> Sequence<T>.delIBiter(størrelse: Int): Sequence<Sequence<T>> =
            sequence {
                val iter = iterator()
                while (iter.hasNext()) {
                    val begrensetIterator = iter.begrense(til = størrelse)
                    val sequenceAvGittStørrelse = begrensetIterator.asSequence()
                    yield(sequenceAvGittStørrelse) // opprett en ny sequence av den begrenset iterator
                    // må gå gjennom elementene av iterator for å unngå Exception og kunne kjøre videre
                    begrensetIterator.forEach { _ -> }
                }
            }

        private fun <T> Iterator<T>.begrense(til: Int): Iterator<T> =
            object : Iterator<T> {
                var left = til
                val iterator by lazy { this@begrense }

                override fun next(): T {
                    if (left == 0) {
                        throw NoSuchElementException()
                    }
                    left--
                    return iterator.next()
                }

                override fun hasNext(): Boolean = left > 0 && iterator.hasNext()
            }
    }
}
