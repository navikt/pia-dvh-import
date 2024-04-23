class NaisEnvironment(
    val statistikkBucketName: String = getEnvVar("STATISTIKK_BUCKET_NAME"),
    val statistikkFileName: String = getEnvVar("STATISTIKK_FILE_NAME")
)

fun getEnvVar(varName: String, defaultValue: String? = null) =
    System.getenv(varName) ?: defaultValue ?: throw RuntimeException("Missing required variable $varName")
