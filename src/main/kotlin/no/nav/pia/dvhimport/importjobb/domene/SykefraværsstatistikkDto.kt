package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
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
    val antallPersoner: Int
}

@Serializable(with = SykefraværsstatistikkDtoSerializer::class)
sealed class SykefraværsstatistikkDto : Sykefraværsstatistikk {
    abstract override val årstall: Int
    abstract override val kvartal: Int
    abstract override val prosent: BigDecimal
    abstract override val tapteDagsverk: BigDecimal
    abstract override val muligeDagsverk: BigDecimal
    abstract override val antallPersoner: Int
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
    override val antallPersoner: Int,
) : SykefraværsstatistikkDto()

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
    override val antallPersoner: Int,
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
    val tapteDagsverkGradert: BigDecimal,
    @Serializable(with = TapteDagsverkPerVarighetListSerializer::class)
    val tapteDagsverkPerVarighet: List<TapteDagsverkPerVarighetDto>,
    override val antallPersoner: Int,
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
    val tapteDagsverkGradert: BigDecimal,
    @Serializable(with = TapteDagsverkPerVarighetListSerializer::class)
    val tapteDagsverkPerVarighet: List<TapteDagsverkPerVarighetDto>,
    override val antallPersoner: Int,
) : SykefraværsstatistikkDto()

@Serializable
data class BransjeSykefraværsstatistikkDto(
    val bransje: String,
    override val årstall: Int,
    override val kvartal: Int,
    @Serializable(with = BigDecimalSerializer::class)
    override val prosent: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    override val tapteDagsverk: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    override val muligeDagsverk: BigDecimal,
    @Serializable(with = BigDecimalSerializer::class)
    val tapteDagsverkGradert: BigDecimal,
    @Serializable(with = TapteDagsverkPerVarighetListSerializer::class)
    val tapteDagsverkPerVarighet: List<TapteDagsverkPerVarighetDto>,
    override val antallPersoner: Int,
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
    val tapteDagsverkGradert: BigDecimal,
    @Serializable(with = TapteDagsverkPerVarighetListSerializer::class)
    val tapteDagsverkPerVarighet: List<TapteDagsverkPerVarighetDto>,
    override val antallPersoner: Int,
    val rectype: String,
) : SykefraværsstatistikkDto()

@Serializable
data class TapteDagsverkPerVarighetDto(
    val varighet: String,
    @Serializable(with = BigDecimalSerializer::class)
    val tapteDagsverk: BigDecimal? = null,
)

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
    override fun serialize(
        encoder: Encoder,
        value: BigDecimal,
    ) = when (encoder) {
        is JsonEncoder -> encoder.encodeJsonElement(JsonUnquotedLiteral(value.toPlainString()))
        else -> encoder.encodeString(value.toPlainString())
    }
}

internal object TapteDagsverkPerVarighetListSerializer : KSerializer<List<TapteDagsverkPerVarighetDto>> {
    private val listSerializer = ListSerializer(TapteDagsverkPerVarighetDto.serializer())
    override val descriptor: SerialDescriptor = listSerializer.descriptor

    override fun serialize(
        encoder: Encoder,
        value: List<TapteDagsverkPerVarighetDto>,
    ) = listSerializer.serialize(encoder, value.filter { it.tapteDagsverk != null })

    override fun deserialize(decoder: Decoder): List<TapteDagsverkPerVarighetDto> = decoder.decodeSerializableValue(listSerializer)
}

object SykefraværsstatistikkDtoSerializer : JsonContentPolymorphicSerializer<SykefraværsstatistikkDto>(SykefraværsstatistikkDto::class) {
    override fun selectDeserializer(element: JsonElement): DeserializationStrategy<SykefraværsstatistikkDto> =
        if (element.jsonObject["land"]?.jsonPrimitive?.content.equals("NO")) {
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

fun String.tilListe(): List<String> =
    Json.decodeFromString<JsonArray>(this).map {
        it.toString()
    }.toList()

inline fun <reified T : Sykefraværsstatistikk> List<String>.toSykefraværsstatistikkDto(): List<T> =
    this.map { it.serializeToSykefraværsstatistikkDto() }.mapAsInstance<T>()

inline fun <reified R> Iterable<*>.mapAsInstance() = map { it.apply { check(this is R) } as R }

fun String.serializeToSykefraværsstatistikkDto(): SykefraværsstatistikkDto =
    Json.decodeFromJsonElement(SykefraværsstatistikkDtoSerializer, Json.parseToJsonElement(this))
