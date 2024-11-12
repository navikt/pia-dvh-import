package no.nav.pia.dvhimport.importjobb.domene

import java.math.BigDecimal
import java.math.RoundingMode

private val ANTALL_SIFRE_I_UTREGNING = 3
private val ANTALL_SIFRE_I_RESULTAT = 1

object StatistikkUtils {
    /**
     * Source of trouth for kalkulering av sykefraværsprosent basert på tapte dagsverk og mulige
     * dagsverk.
     */
    fun kalkulerSykefraværsprosent(
        dagsverkTeller: BigDecimal?,
        dagsverkNevner: BigDecimal?,
    ): BigDecimal {
        if (dagsverkTeller == null || dagsverkNevner == null || dagsverkNevner.compareTo(BigDecimal.ZERO) == 0) {
            throw RuntimeException("Kan ikke kalkulere statistikk")
        }

        return dagsverkTeller
            .divide(dagsverkNevner, ANTALL_SIFRE_I_UTREGNING, RoundingMode.HALF_UP)
            .multiply(BigDecimal(100))
            .setScale(ANTALL_SIFRE_I_RESULTAT, RoundingMode.HALF_UP)
    }
}
