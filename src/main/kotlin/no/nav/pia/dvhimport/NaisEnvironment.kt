package no.nav.pia.dvhimport

class NaisEnvironment(
    val googleCloudStorageUrl: String = getEnvVar("GCS_URL"),
    val statistikkBucketName: String = getEnvVar("GCS_SYKEFRAVARSSTATISTIKK_BUCKET_NAME"),
    val databaseJdbcUrl: String = getEnvVar("NAIS_DATABASE_PIA_DVH_IMPORT_PIA_DVH_IMPORT_DB_JDBC_URL"),
    val naisClusterName: String = getEnvVar("NAIS_CLUSTER_NAME", "lokal"),
    val slackWebhookUrl: String = getEnvVar("SLACK_WEBHOOK_URL", ""),
) {
    companion object {
        fun getEnvVar(
            varName: String,
            defaultValue: String? = null,
        ) = System.getenv(varName) ?: defaultValue ?: throw RuntimeException("Missing required variable $varName")
    }
}
