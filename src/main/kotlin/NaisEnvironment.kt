class NaisEnvironment(
    val statistikkBucketName: String = getEnvVar("STATISTIKK_BUCKET_NAME"),
)

fun getEnvVar(varName: String, defaultValue: String? = null) =
    System.getenv(varName) ?: defaultValue ?: throw RuntimeException("Missing required variable $varName")
