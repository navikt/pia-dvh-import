package no.nav.pia.dvhimport.importjobb.kafka

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.importjobb.domene.Statistikkategori
import no.nav.pia.dvhimport.importjobb.domene.SykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetMetadataDto
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class EksportProdusent(kafkaConfig: KafkaConfig) {
    private val producer: KafkaProducer<String, String> = KafkaProducer(kafkaConfig.producerProperties())

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            producer.close()
        })
    }

    fun <T> sendMelding(melding: EksportMelding<T>) {
        val topic = when (melding) {
            is SykefraværsstatistikkMelding -> if (melding.ekstraNøkkel != Statistikkategori.VIRKSOMHET.name) {
                KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER.navnMedNamespace
            } else {
                KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET.navnMedNamespace
            }
            is VirksomhetMetadataMelding -> KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET_METADATA.navnMedNamespace
            is PubliseringsdatoMelding -> "Not yet implemented" //TODO: Implement
        }
        producer.send(
            ProducerRecord(
                topic,
                melding.tilNøkkel(),
                melding.tilMelding()
            )
        )
    }

    class VirksomhetMetadataMelding(
        kvartal: String,
        virksomhetMetadata: VirksomhetMetadataDto,
    ) : EksportMelding<VirksomhetMetadataDto>(
        kvartal = kvartal,
        meldingType = MeldingType.METADATA_FOR_VIRKSOMHET,
        data = virksomhetMetadata,
    )

    class SykefraværsstatistikkMelding(
        kvartal: String,
        statistikkategori: Statistikkategori,
        sykefraværsstatistikk: SykefraværsstatistikkDto,
    ) : EksportMelding<SykefraværsstatistikkDto>(
        kvartal = kvartal,
        meldingType = MeldingType.SYKEFRAVÆRSSTATISTIKK,
        ekstraNøkkel = statistikkategori.name,
        data = sykefraværsstatistikk,
    )

    class PubliseringsdatoMelding(
        kvartal: String,
        toBeDefined: String,
    ) : EksportMelding<String>(
        kvartal = kvartal,
        meldingType = MeldingType.PUBLISERINGSDATO,
        data = toBeDefined
    )

    @Serializable
    sealed class EksportMelding<T>(
        val kvartal: String,
        val meldingType: MeldingType,
        val ekstraNøkkel: String = "",
        val data: T,
    ) {
        fun tilNøkkel() =
            Json.encodeToString(
                SykefraværsstatistikkNøkkel(
                    kvartal,
                    if (ekstraNøkkel.isNotEmpty()) {
                        this.meldingType.name + "-" + this.ekstraNøkkel
                    } else {
                        this.meldingType.name
                    }
                )
            )

        fun tilMelding() =
            when (this) {
                is SykefraværsstatistikkMelding -> Json.encodeToString<SykefraværsstatistikkDto>(data)
                is VirksomhetMetadataMelding -> Json.encodeToString<VirksomhetMetadataDto>(data)
                is PubliseringsdatoMelding -> Json.encodeToString<String>(data)
            }
    }

    enum class MeldingType {
        SYKEFRAVÆRSSTATISTIKK,
        METADATA_FOR_VIRKSOMHET,
        PUBLISERINGSDATO,
    }

    @Serializable
    data class SykefraværsstatistikkNøkkel(
        val kvartal: String,
        val meldingType: String,
    )
}
