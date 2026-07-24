package no.nav.pia.dvhimport.importjobb.orkestrering

import java.math.BigDecimal
import java.time.LocalDateTime

data class ImportLockDto(
    val id: Int,
    val publiseringsdatoId: Int,
    val status: ImportLockStatus,
    val startDato: LocalDateTime,
    val sluttDato: LocalDateTime?,
)

data class ImportStegDto(
    val id: Int,
    val publiseringsdatoId: Int,
    val steg: ImportSteg,
    val rekkefolge: Int,
    val status: ImportStegStatus,
    val kontroll: Kontroll?,
    val startDato: LocalDateTime?,
    val sluttDato: LocalDateTime?,
    val antallRaderLest: Int,
    val antallSendtPaaKafka: Int,
    val sfProsent: BigDecimal?,
)
