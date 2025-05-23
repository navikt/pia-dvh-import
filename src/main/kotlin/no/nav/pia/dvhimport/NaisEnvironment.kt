package no.nav.pia.dvhimport

class NaisEnvironment(
    val googleCloudStorageUrl: String = getEnvVar("GCS_URL"),
    val statistikkBucketName: String = getEnvVar("GCS_SYKEFRAVARSSTATISTIKK_BUCKET_NAME"),
) {
    companion object {
        fun getEnvVar(
            varName: String,
            defaultValue: String? = null,
        ) = System.getenv(varName) ?: defaultValue ?: throw RuntimeException("Missing required variable $varName")
    }
}
