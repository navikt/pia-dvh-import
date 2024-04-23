package no.nav.fia.arbeidsgiver.konfigurasjon

import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig.Companion.clientId


enum class KafkaTopics(
    val navn: String,
    private val prefix: String = "pia",
) {
    DVH_IMPORT_JOBBLYTTER("jobblytter-v1"),;

    val konsumentGruppe
        get() = "${navn}_$clientId"

    val navnMedNamespace
        get() = "${prefix}.${navn}"
}
