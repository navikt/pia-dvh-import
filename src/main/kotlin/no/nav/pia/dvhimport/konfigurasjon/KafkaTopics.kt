package no.nav.pia.dvhimport.konfigurasjon

import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig.Companion.CLIENT_ID

enum class KafkaTopics(
    val navn: String,
    private val prefix: String = "pia",
) {
    PIA_JOBBLYTTER("jobblytter-v1"),
    KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER("kvartalsvis-sykefravarsstatistikk-ovrige-kategorier-v1"),
    KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET("kvartalsvis-sykefravarsstatistikk-virksomhet-v1"),
    KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET_METADATA("kvartalsvis-sykefravarsstatistikk-virksomhet-metadata-v1"),
    KVARTALSVIS_SYKEFRAVARSSTATISTIKK_PUBLISERINGSDATO("kvartalsvis-sykefravarsstatistikk-publiseringsdato-v1"), ;

    val konsumentGruppe
        get() = "${navn}_$CLIENT_ID"

    val navnMedNamespace
        get() = "$prefix.$navn"
}
