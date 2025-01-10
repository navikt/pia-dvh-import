package no.nav.pia.dvhimport.importjobb.domene

data class ÅrstallOgKvartal(
    val årstall: Int,
    val kvartal: Int,
) : Comparable<ÅrstallOgKvartal> {
    init {
        require(!(kvartal > 4 || kvartal < 1)) { "Kvartal må være 1, 2, 3 eller 4" }
    }

    fun minusKvartaler(antallKvartaler: Int): ÅrstallOgKvartal {
        if (antallKvartaler < 0) {
            return plussKvartaler(-antallKvartaler)
        }
        var årstallOgKvartal = ÅrstallOgKvartal(årstall, kvartal)
        for (i in 0 until antallKvartaler) {
            årstallOgKvartal = årstallOgKvartal.forrigeKvartal()
        }
        return årstallOgKvartal
    }

    fun plussKvartaler(antallKvartaler: Int): ÅrstallOgKvartal {
        if (antallKvartaler < 0) {
            return minusKvartaler(-antallKvartaler)
        }
        var årstallOgKvartal = ÅrstallOgKvartal(årstall, kvartal)
        for (i in 0 until antallKvartaler) {
            årstallOgKvartal = årstallOgKvartal.nesteKvartal()
        }
        return årstallOgKvartal
    }

    private fun forrigeKvartal(): ÅrstallOgKvartal =
        if (kvartal == 1) {
            ÅrstallOgKvartal(årstall - 1, 4)
        } else {
            ÅrstallOgKvartal(årstall, kvartal - 1)
        }

    private fun nesteKvartal(): ÅrstallOgKvartal =
        if (kvartal == 4) {
            ÅrstallOgKvartal(årstall + 1, 1)
        } else {
            ÅrstallOgKvartal(årstall, kvartal + 1)
        }

    override fun compareTo(other: ÅrstallOgKvartal): Int =
        Comparator.comparing<ÅrstallOgKvartal, Int> { it.årstall }
            .thenComparing<Int> { it.kvartal }
            .compare(this, other)

    override fun toString(): String = "$kvartal. kvartal $årstall"
}
