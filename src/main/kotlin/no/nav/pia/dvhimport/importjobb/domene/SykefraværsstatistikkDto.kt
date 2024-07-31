package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonContentPolymorphicSerializer
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonUnquotedLiteral
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.math.BigDecimal


sealed interface Sykefraværsstatistikk {
    val årstall: Int
    val kvartal: Int
    val prosent: BigDecimal
    val tapteDagsverk: BigDecimal
    val muligeDagsverk: BigDecimal
    val antallPersoner: BigDecimal
}

@Serializable(with = SykefraværsstatistikkDtoSerializer::class)
sealed class SykefraværsstatistikkDto: Sykefraværsstatistikk {
    override abstract val årstall: Int
    override abstract val kvartal: Int
    override abstract val prosent: BigDecimal
    override abstract val tapteDagsverk: BigDecimal
    override abstract val muligeDagsverk: BigDecimal
    override abstract val antallPersoner: BigDecimal
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
): SykefraværsstatistikkDto()

@Serializable
data class SektorSykefraværsstatistikkDto(
    val sektor: String,
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
) : SykefraværsstatistikkDto()

@Serializable
data class NæringSykefraværsstatistikkDto(
    val næring: String,
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
) : SykefraværsstatistikkDto()

@Serializable
data class NæringskodeSykefraværsstatistikkDto(
    val næringskode: String,
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
) : SykefraværsstatistikkDto()

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
) : SykefraværsstatistikkDto()


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

object SykefraværsstatistikkDtoSerializer : JsonContentPolymorphicSerializer<SykefraværsstatistikkDto>(SykefraværsstatistikkDto::class) {
    override fun selectDeserializer(element: JsonElement): DeserializationStrategy<SykefraværsstatistikkDto> {
        return if (element.jsonObject["land"]?.jsonPrimitive?.content.equals("NO")) {
            LandSykefraværsstatistikkDto.serializer()
        } else if (element.jsonObject["sektor"]?.jsonPrimitive?.isString == true) {
            SektorSykefraværsstatistikkDto.serializer()
        } else if (element.jsonObject["næring"]?.jsonPrimitive?.isString == true) {
            NæringSykefraværsstatistikkDto.serializer()
        } else if (element.jsonObject["næringskode"]?.jsonPrimitive?.isString == true) {
            NæringskodeSykefraværsstatistikkDto.serializer()
        } else if (element.jsonObject["orgnr"]?.jsonPrimitive?.isString == true) {
            VirksomhetSykefraværsstatistikkDto.serializer()
        } else {
            throw Exception("Ukjent kategori for statistikk")
        }
    }
}

fun String.tilGeneriskStatistikk(): List<String> =
    Json.decodeFromString<JsonArray>(this).map {
        it.toString()
    }.toList()


inline fun <reified T> List<String>.toSykefraværsstatistikkDto(): List<T> =
    this.map { it.serializeToSykefraværsstatistikkDto() }.mapAsInstance<T>()

inline fun <reified R> Iterable<*>.mapAsInstance() = map { it.apply { check(this is R) } as R }

fun String.serializeToSykefraværsstatistikkDto(): SykefraværsstatistikkDto =
    Json.decodeFromJsonElement(SykefraværsstatistikkDtoSerializer, Json.parseToJsonElement(this))

