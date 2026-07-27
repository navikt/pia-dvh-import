package no.nav.pia.dvhimport.importjobb.orkestrering

import java.math.BigDecimal

data class StegValideringsresultat(
    val antallRaderLest: Int,
    val sfProsent: BigDecimal?,
)
