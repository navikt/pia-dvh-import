package no.nav.pia.dvhimport.helper

import com.google.cloud.NoCredentials
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions
import com.google.common.net.MediaType
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.Testcontainers
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy


class GoogleCloudStorageContainerHelper(
    network: Network = Network.newNetwork(),
    log: Logger = LoggerFactory.getLogger(GoogleCloudStorageContainerHelper::class.java),
) {

    private val DEFAULT_GCS_BUCKET_I_TESTCONTAINER = "fake-gcs-bucket-in-container"
    private val gcsNetworkAlias = "gcsContainer"
    private val defaultport = 4443
    private val baseUrl = "http://$gcsNetworkAlias:$defaultport"
    private val localLogger: Logger = LoggerFactory.getLogger(GoogleCloudStorageContainerHelper::class.java)

    private lateinit var storage: Storage

    val container = FakeGcsServerContainer()
        .withExposedPorts(defaultport)
        .withNetwork(network)
        .withNetworkAliases(gcsNetworkAlias)
        .withNetwork(network)
        .withLogConsumer(Slf4jLogConsumer(log).withPrefix(gcsNetworkAlias).withSeparateOutputStreams())
        .withCreateContainerCmdModifier { cmd -> cmd.withName("$gcsNetworkAlias-${System.currentTimeMillis()}") }
        .waitingFor(HostPortWaitStrategy())
        .withReuse(true)
        .apply {
            start()
            Testcontainers.exposeHostPorts(firstMappedPort, 4443)
        }

    fun envVars(): Map<String, String> {
        return mapOf(
            "GCS_URL" to baseUrl,
            "GCS_SYKEFRAVARSSTATISTIKK_BUCKET_NAME" to DEFAULT_GCS_BUCKET_I_TESTCONTAINER,
        )
    }

    fun opprettTestBucketHvisIkkeFunnet(
        bucketName: String = DEFAULT_GCS_BUCKET_I_TESTCONTAINER
    ) {
        try {
            val bucket = storage.get(bucketName)
            if (!bucket.exists() || bucket.asBucketInfo().name != bucketName) {
                this.storage = createBucketAndGetStorage(bucketName = bucketName)
            }
        } catch (ex: Exception) {
            when (ex) {
                is UninitializedPropertyAccessException,
                is StorageException ->
                    this.storage = createBucketAndGetStorage(bucketName = DEFAULT_GCS_BUCKET_I_TESTCONTAINER)
                else -> throw ex
            }
        }
    }


    fun createBucketAndGetStorage(
        bucketName: String = DEFAULT_GCS_BUCKET_I_TESTCONTAINER
    ): Storage {
        val projectId = "fake-google-cloud-storage-container-project"
        val url = container.url()
        val storage = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setHost(url)
            .setProjectId(projectId)
            .build()
            .service

        storage.create(BucketInfo.of(bucketName))
        localLogger.info("Opprettet bucket $bucketName")
        return storage
    }


    fun lagreTestBlob(
        blobNavn: String,
        bucketName: String = DEFAULT_GCS_BUCKET_I_TESTCONTAINER,
        contentType: MediaType = MediaType.JSON_UTF_8,
        metadata: Map<String, String> = emptyMap(),
        bytes: ByteArray,
    ) {
        runBlocking {
            val contentTypeVerdi = contentType.toString()
            val blobInfo = BlobInfo.newBuilder(bucketName, blobNavn).setContentType(contentTypeVerdi)
                .setMetadata(metadata + mapOf("content-type" to contentTypeVerdi)).build()

            return@runBlocking storage.create(blobInfo, bytes)
        }
    }

    fun verifiserBlobFinnes(
        blobNavn: String,
        bucketName: String = DEFAULT_GCS_BUCKET_I_TESTCONTAINER,
    ): Boolean {
        val result = runBlocking {
            val blob: Blob = storage.get(bucketName, blobNavn)
            return@runBlocking blob.exists()
        }
        return result
    }
}
