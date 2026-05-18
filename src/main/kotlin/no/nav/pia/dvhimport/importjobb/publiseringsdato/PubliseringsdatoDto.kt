package no.nav.pia.dvhimport.importjobb.publiseringsdato

import java.time.LocalDate

data class PubliseringsdatoDto(
    val id: Int,
    val årstall: Int,
    val kvartal: Int,
    val dato: LocalDate,
    val prosessert: Boolean,
)

enum class LagreResultat { NY, OPPDATERT, UENDRET }
