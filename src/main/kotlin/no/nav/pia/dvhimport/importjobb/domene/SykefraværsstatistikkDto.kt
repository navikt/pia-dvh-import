package no.nav.pia.dvhimport.importjobb.domene

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class SykefraværsstatistikkDto(
    val orgnr: String,
    @SerialName("arstall")
    val årstall: Int,
    val kvartal: Int,
    @SerialName("taptedv")
    val tapteDagsverk: Int,
    @SerialName("muligedv")
    val muligeDagsverk: Int,
    @SerialName("antpers")
    val antallPersoner: Int,
) {
    constructor() : this(
        "",
        2023,
        1,
        0,
        0,
        0
        )
}