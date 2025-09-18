package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.daysUntil
import kotlinx.datetime.format
import kotlinx.datetime.format.char
import kotlinx.datetime.toInstant
import kotlinx.datetime.toJavaLocalDateTime
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlin.time.ExperimentalTime

@Serializable
data class PubliseringsdatoFraDvhDto(
    @SerialName("rapport_periode")
    val rapportPeriode: String,
    @SerialName("offentlig_dato")
    @Serializable(with = DvhDatoMedTidSerializer::class)
    val offentligDato: LocalDateTime,
    @SerialName("oppdatert_i_dvh")
    @Serializable(with = DvhDatoMedTidSerializer::class)
    val oppdatertIDvh: LocalDateTime,
)

@Serializable
data class PubliseringsdatoDto(
    val rapportPeriode: String,
    val offentligDato: LocalDateTime,
    val oppdatertIDvh: LocalDateTime,
)

data class Publiseringsdato(
    val årstall: Int,
    val kvartal: Int,
    val offentligDato: LocalDateTime,
) {
    companion object {
        val timeZone = TimeZone.of("Europe/Oslo")

        @OptIn(ExperimentalTime::class)
        fun LocalDateTime.antallDagerTilPubliseringsdato(publiseringsdato: Publiseringsdato): Int {
            val fraInstant = this.toInstant(timeZone)
            val tilInstant = publiseringsdato.offentligDato.toInstant(timeZone)

            return fraInstant.daysUntil(tilInstant, timeZone)
        }

        fun LocalDateTime.erFørPubliseringsdato(publiseringsdato: Publiseringsdato): Boolean =
            this.toJavaLocalDateTime().toLocalDate()
                .isBefore(publiseringsdato.offentligDato.toJavaLocalDateTime().toLocalDate())

        fun sjekkPubliseringErIDag(
            publiseringsdatoer: List<PubliseringsdatoFraDvhDto>,
            iDag: LocalDateTime,
        ): PubliseringsdatoFraDvhDto? =
            publiseringsdatoer.find {
                it.offentligDato.toJavaLocalDateTime().toLocalDate()
                    .isEqual(iDag.toJavaLocalDateTime().toLocalDate())
            }
    }
}

fun PubliseringsdatoFraDvhDto.tilPubliseringsdato(): Publiseringsdato =
    Publiseringsdato(
        årstall = this.rapportPeriode.subSequence(0, 4).toString().toInt(),
        kvartal = this.rapportPeriode.subSequence(5, 6).toString().toInt(),
        offentligDato = offentligDato,
    )

data class NestePubliseringsdato(
    val kvartal: Int,
    val årstall: Int,
    val dato: LocalDateTime,
)

internal object DvhDatoMedTidSerializer : KSerializer<LocalDateTime> {
    override val descriptor = PrimitiveSerialDescriptor("LocalDateTime", PrimitiveKind.STRING)

    val dvhTidsformat = LocalDateTime.Format {
        date(LocalDate.Formats.ISO)
        char(',')
        char(' ')
        hour()
        char(':')
        minute()
        char(':')
        second()
    }

    override fun serialize(
        encoder: Encoder,
        value: LocalDateTime,
    ) {
        encoder.encodeString(value.format(LocalDateTime.Formats.ISO))
    }

    override fun deserialize(decoder: Decoder): LocalDateTime = LocalDateTime.parse(decoder.decodeString(), dvhTidsformat)
}

fun PubliseringsdatoFraDvhDto.tilPubliseringsdatoDto(): PubliseringsdatoDto =
    PubliseringsdatoDto(
        rapportPeriode = this.rapportPeriode,
        offentligDato = this.offentligDato,
        oppdatertIDvh = this.oppdatertIDvh,
    )

fun List<String>.tilPubliseringsdatoFraDvhDto(): List<PubliseringsdatoFraDvhDto> = this.map { it.tilPubliseringsdatoFraDvhDto() }

fun String.tilPubliseringsdatoFraDvhDto(): PubliseringsdatoFraDvhDto = Json.decodeFromString<PubliseringsdatoFraDvhDto>(this)
