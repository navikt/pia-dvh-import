package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonUnquotedLiteral
import kotlinx.serialization.json.jsonPrimitive
import java.math.BigDecimal

@Serializable
data class SykefraværsstatistikkDto(
    @SerialName("ARSTALL")
    val årstall: Int,
    @SerialName("KVARTAL")
    val kvartal: Int,
    @SerialName("ORGNR")
    val orgnr: String,
    @SerialName("NARING")
    val næring: String,
    @SerialName("NARING_KODE")
    val næringskode: String,
    @SerialName("PRIMARNARINGSKODE")
    val primærnæringskode: String,
    @SerialName("SEKTOR")
    val sektor: String,
    @SerialName("VARIGHET")
    val varighet: String,
    @SerialName("RECTYPE")
    val rectype: String,
    @SerialName("TAPTEDV")
    @Serializable(with = BigDecimalSerializer::class)
    val tapteDagsverk: BigDecimal,
    @SerialName("MULIGEDV")
    @Serializable(with = BigDecimalSerializer::class)
    val muligeDagsverk: BigDecimal,
    @SerialName("ANTALL_GS")
    @Serializable(with = BigDecimalSerializer::class)
    val antallDagsverkGs: BigDecimal,
    @SerialName("TAPTEDV_GS")
    @Serializable(with = BigDecimalSerializer::class)
    val tapteDagsverkGs: BigDecimal,
    @SerialName("ANTPERS")
    val antallPersoner: Int,
)

@OptIn(ExperimentalSerializationApi::class)
private object BigDecimalSerializer : KSerializer<BigDecimal> {

    override val descriptor = PrimitiveSerialDescriptor("java.math.BigDecimal", PrimitiveKind.DOUBLE)

    /**
     * Parse til en [BigDecimal] fra raw content med [JsonDecoder.decodeJsonElement],
     *  eller med [Decoder.decodeString] hvis verdien kommer som en [String]
     */
    override fun deserialize(decoder: Decoder): BigDecimal =
        when (decoder) {
            is JsonDecoder -> decoder.decodeJsonElement().jsonPrimitive.content.toBigDecimal()
            else -> decoder.decodeString().toBigDecimal()
        }

    /**
     * Bruk av [JsonUnquotedLiteral] for å produsere en [BigDecimal] verdi uten ""
     *  eller, produserer en [value] med "" ved å bruke [Encoder.encodeString].
     */
    override fun serialize(encoder: Encoder, value: BigDecimal) =
        when (encoder) {
            is JsonEncoder -> encoder.encodeJsonElement(JsonUnquotedLiteral(value.toPlainString()))
            else -> encoder.encodeString(value.toPlainString())
        }
}
