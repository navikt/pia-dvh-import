package no.nav.pia.dvhimport.importjobb.kafka

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import no.nav.pia.dvhimport.importjobb.domene.BransjeSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.LandSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.NæringSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.NæringskodeSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.PubliseringsdatoDto
import no.nav.pia.dvhimport.importjobb.domene.SektorSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori.BRANSJE
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori.LAND
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori.NÆRING
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori.NÆRINGSKODE
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori.SEKTOR
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori.VIRKSOMHET
import no.nav.pia.dvhimport.importjobb.domene.SykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetMetadataDto
import no.nav.pia.dvhimport.importjobb.domene.VirksomhetSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.MeldingType.METADATA_FOR_VIRKSOMHET
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.MeldingType.PUBLISERINGSDATO
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.MeldingType.SYKEFRAVÆRSSTATISTIKK
import no.nav.pia.dvhimport.konfigurasjon.KafkaConfig
import no.nav.pia.dvhimport.konfigurasjon.KafkaTopics
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class EksportProdusent(
    kafkaConfig: KafkaConfig,
) {
    private val producer: KafkaProducer<String, String> = KafkaProducer(kafkaConfig.producerProperties())

    init {
        Runtime.getRuntime().addShutdownHook(
            Thread {
                producer.close()
            },
        )
    }

    fun <T> sendMelding(melding: EksportMelding<T>) {
        val topic = when (melding) {
            is SykefraværsstatistikkMelding -> if (melding.isViksomhetStatistikk()) {
                KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET.navnMedNamespace
            } else {
                KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_ØVRIGE_KATEGORIER.navnMedNamespace
            }

            is VirksomhetMetadataMelding -> KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_VIRKSOMHET_METADATA.navnMedNamespace
            is PubliseringsdatoMelding -> KafkaTopics.KVARTALSVIS_SYKEFRAVARSSTATISTIKK_PUBLISERINGSDATO.navnMedNamespace
        }
        val nøkkel = melding.tilNøkkel()
        val content = melding.tilMelding()

        producer.send(
            ProducerRecord(
                topic,
                nøkkel,
                content,
            ),
        )
    }

    class VirksomhetMetadataMelding(
        årstall: Int,
        kvartal: Int,
        virksomhetMetadata: VirksomhetMetadataDto,
    ) : EksportMelding<VirksomhetMetadataDto>(
            årstall = årstall,
            kvartal = kvartal,
            meldingType = METADATA_FOR_VIRKSOMHET,
            data = virksomhetMetadata,
        )

    class SykefraværsstatistikkMelding<T>(
        årstall: Int,
        kvartal: Int,
        sykefraværsstatistikk: T,
    ) : EksportMelding<T>(
            årstall = årstall,
            kvartal = kvartal,
            meldingType = SYKEFRAVÆRSSTATISTIKK,
            data = sykefraværsstatistikk,
        )

    class PubliseringsdatoMelding(
        årstall: Int,
        kvartal: Int,
        publiseringsdato: PubliseringsdatoDto,
    ) : EksportMelding<PubliseringsdatoDto>(
            årstall = årstall,
            kvartal = kvartal,
            meldingType = PUBLISERINGSDATO,
            data = publiseringsdato,
        )

    @Serializable
    sealed class EksportMelding<T>(
        val årstall: Int,
        val kvartal: Int,
        val meldingType: MeldingType,
        val data: T,
    ) {
        fun isViksomhetStatistikk(): Boolean =
            when (data) {
                is SykefraværsstatistikkDto -> data.kategori() == VIRKSOMHET
                else -> {
                    false
                }
            }

        private fun SykefraværsstatistikkDto.tilStatistikkSpesifikkVerdi() =
            when (this) {
                is LandSykefraværsstatistikkDto -> this.land
                is SektorSykefraværsstatistikkDto -> this.sektor
                is NæringSykefraværsstatistikkDto -> this.næring
                is NæringskodeSykefraværsstatistikkDto -> this.næringskode
                is BransjeSykefraværsstatistikkDto -> this.bransje
                is VirksomhetSykefraværsstatistikkDto -> this.orgnr
            }

        private fun SykefraværsstatistikkDto.kategori(): StatistikkKategori =
            when (this) {
                is LandSykefraværsstatistikkDto -> LAND
                is SektorSykefraværsstatistikkDto -> SEKTOR
                is NæringSykefraværsstatistikkDto -> NÆRING
                is NæringskodeSykefraværsstatistikkDto -> NÆRINGSKODE
                is BransjeSykefraværsstatistikkDto -> BRANSJE
                is VirksomhetSykefraværsstatistikkDto -> VIRKSOMHET
            }

        private fun tilNøkkelverdi(): String =
            when (data) {
                is SykefraværsstatistikkDto -> data.tilStatistikkSpesifikkVerdi()
                is VirksomhetMetadataDto -> data.orgnr
                is PubliseringsdatoDto -> data.rapportPeriode
                else -> {
                    throw RuntimeException("Kunne ikke hente kode verdi for '${data!!::class.java.name}'")
                }
            }

        private fun statistikkKategori(): StatistikkKategori =
            when (data) {
                is SykefraværsstatistikkDto -> data.kategori()
                else -> {
                    throw RuntimeException("Kan ikke hente statistikk kategori for '${data!!::class.java.name}'")
                }
            }

        fun tilNøkkel() =
            when (meldingType) {
                SYKEFRAVÆRSSTATISTIKK -> Json.encodeToString<SykefraværsstatistikkNøkkel>(
                    SykefraværsstatistikkNøkkel(
                        årstall = this.årstall,
                        kvartal = this.kvartal,
                        kategori = statistikkKategori(),
                        kode = tilNøkkelverdi(),
                    ),
                )

                METADATA_FOR_VIRKSOMHET -> Json.encodeToString<VirksomhetMetadataNøkkel>(
                    VirksomhetMetadataNøkkel(
                        årstall = this.årstall,
                        kvartal = this.kvartal,
                        orgnr = tilNøkkelverdi(),
                    ),
                )

                PUBLISERINGSDATO -> Json.encodeToString<PubliseringsdatoNøkkel>(
                    PubliseringsdatoNøkkel(
                        årstall = this.årstall,
                        rapportPeriode = tilNøkkelverdi(),
                    ),
                )
            }

        fun tilMelding() =
            when (this) {
                is SykefraværsstatistikkMelding -> Json.encodeToString<SykefraværsstatistikkDto>(data as SykefraværsstatistikkDto)
                is VirksomhetMetadataMelding -> Json.encodeToString<VirksomhetMetadataDto>(data)
                is PubliseringsdatoMelding -> Json.encodeToString<PubliseringsdatoDto>(data)
            }
    }

    enum class MeldingType {
        SYKEFRAVÆRSSTATISTIKK,
        METADATA_FOR_VIRKSOMHET,
        PUBLISERINGSDATO,
    }

    @Serializable
    data class SykefraværsstatistikkNøkkel(
        val årstall: Int,
        val kvartal: Int,
        val kategori: StatistikkKategori,
        val kode: String,
    )

    @Serializable
    data class VirksomhetMetadataNøkkel(
        val årstall: Int,
        val kvartal: Int,
        val orgnr: String,
    )

    @Serializable
    data class PubliseringsdatoNøkkel(
        val årstall: Int,
        val rapportPeriode: String,
    )
}
