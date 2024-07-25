package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
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

interface SykefraværsstatistikkDto {
    val årstall: Int
    val kvartal: Int
    val prosent: BigDecimal
    val tapteDagsverk: BigDecimal
    val muligeDagsverk: BigDecimal
    val antallPersoner: BigDecimal
}

@Serializable
data class LandSykefraværsstatistikkDto(
    val land: String,
    override val årstall: Int,
    override val kvartal: Int,
    @Serializable(with = BigDecimalSerializer::class)
    override val prosent: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    override val tapteDagsverk: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    override val muligeDagsverk: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    override val antallPersoner: BigDecimal,
): SykefraværsstatistikkDto

@Serializable
data class TapteDagsverkPerVarighetDto(
    val varighet: String,
    @Serializable(with = BigDecimalSerializer::class)
    val tapteDagsverk: BigDecimal,
)

@Serializable
data class VirksomhetSykefraværsstatistikkDto(
    val orgnr: String,
    override val årstall: Int,
    override val kvartal: Int,
    @Serializable(with = BigDecimalSerializer::class)
    override val prosent: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    override val tapteDagsverk: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    override val muligeDagsverk: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    val varighet_a: BigDecimal? = null,
    @Serializable(with = BigDecimalSerializer::class)
    val varighet_b: BigDecimal? = null,
    @Serializable(with = BigDecimalSerializer::class)
    val varighet_c: BigDecimal? = null,
    @Serializable(with = BigDecimalSerializer::class)
    val varighet_d: BigDecimal? = null,
    @Serializable(with = BigDecimalSerializer::class)
    val varighet_e: BigDecimal? = null,
    @Serializable(with = BigDecimalSerializer::class)
    val varighet_f: BigDecimal? = null,
    @Serializable(with = BigDecimalSerializer::class)
    override val antallPersoner: BigDecimal,
    val rectype: String,
) : SykefraværsstatistikkDto


@OptIn(ExperimentalSerializationApi::class)
internal object BigDecimalSerializer : KSerializer<BigDecimal> {

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
