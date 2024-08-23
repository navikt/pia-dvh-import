package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.format
import kotlinx.datetime.format.char
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json

@Serializable
data class PubliseringsdatoDto(
    @SerialName("rapport_periode")
    val rapportPeriode: String,
    @SerialName("offentlig_dato")
    @Serializable(with = LocalDateTimeSerializer::class)
    val offentligDato: LocalDateTime,
    @SerialName("oppdatert_i_dvh")
    val oppdatertIDvh: LocalDate,
)

internal object LocalDateTimeSerializer : KSerializer<LocalDateTime> {
    override val descriptor = PrimitiveSerialDescriptor("LocalDateTime", PrimitiveKind.STRING)

    val customFormat = LocalDateTime.Format {
        date(LocalDate.Formats.ISO)
        char(',')
        char(' ')
        hour(); char(':'); minute(); char(':'); second()
    }

    override fun serialize(encoder: Encoder, value: LocalDateTime) {
        encoder.encodeString(value.format(customFormat))
    }

    override fun deserialize(decoder: Decoder): LocalDateTime {
        return LocalDateTime.parse(decoder.decodeString(), customFormat)
    }
}

fun List<String>.tilPubliseringsdatoDto(): List<PubliseringsdatoDto> =
    this.map { it.tilPubliseringsdatoDto() }

fun String.tilPubliseringsdatoDto(): PubliseringsdatoDto =
    Json.decodeFromString<PubliseringsdatoDto>(this)