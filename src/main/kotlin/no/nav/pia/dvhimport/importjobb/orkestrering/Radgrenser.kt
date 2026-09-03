package no.nav.pia.dvhimport.importjobb.orkestrering

data class Radgrense(
    val nedre: Int,
    val øvre: Int,
) {
    fun inneholder(antall: Int): Boolean = antall in nedre..øvre
}

class Radgrenser(
    private val perSteg: Map<ImportSteg, Radgrense>,
) {
    fun forSteg(steg: ImportSteg): Radgrense = perSteg.getValue(steg)

    companion object {
        private val PROD: Map<ImportSteg, Radgrense> = mapOf(
            ImportSteg.IMPORT_LAND to Radgrense(1, 1),
            ImportSteg.IMPORT_SEKTOR to Radgrense(3, 5),
            ImportSteg.IMPORT_NARING to Radgrense(50, 150),
            ImportSteg.IMPORT_NARINGSKODE to Radgrense(500, 1500),
            ImportSteg.IMPORT_BRANSJE to Radgrense(50, 150),
            ImportSteg.IMPORT_VIRKSOMHET to Radgrense(250_000, 500_000),
            ImportSteg.IMPORT_VIRKSOMHET_METADATA to Radgrense(250_000, 500_000),
        )

        // Kun VIRKSOMHET/METADATA skiller seg fra prod.
        private val DEV: Map<ImportSteg, Radgrense> = PROD + mapOf(
            ImportSteg.IMPORT_VIRKSOMHET to Radgrense(1_000, 3_000),
            ImportSteg.IMPORT_VIRKSOMHET_METADATA to Radgrense(1_000, 3_000),
        )

        // Brukes kun til testcontainers, og har ingen reelle grenser (0, MAX).
        private val LOKAL: Map<ImportSteg, Radgrense> =
            ImportSteg.entries.associateWith { Radgrense(0, Int.MAX_VALUE) }

        fun forCluster(naisClusterName: String): Radgrenser =
            when (naisClusterName) {
                "prod-gcp" -> Radgrenser(PROD)
                "dev-gcp" -> Radgrenser(DEV)
                else -> Radgrenser(LOKAL)
            }
    }
}
