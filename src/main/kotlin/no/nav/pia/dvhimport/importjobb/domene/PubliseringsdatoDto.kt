package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class PubliseringsdatoDto(
    @SerialName("rapport_periode")
    val rapportPeriode: String,
    @SerialName("offentlig_dato")
    val offentligDato: String,
    @SerialName("oppdatert_i_dvh")
    val oppdatertIDvh: String,
)

fun List<String>.tilPubliseringsdatoDto(): List<PubliseringsdatoDto> =
    this.map { it.tilPubliseringsdatoDto() }

fun String.tilPubliseringsdatoDto(): PubliseringsdatoDto =
    Json.decodeFromString<PubliseringsdatoDto>(this)