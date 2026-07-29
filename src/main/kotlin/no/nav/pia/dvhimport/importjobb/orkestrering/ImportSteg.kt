package no.nav.pia.dvhimport.importjobb.orkestrering

enum class ImportSteg(
    val rekkefolge: Int,
    val visningsnavn: String,
    val strukturRegex: Regex?,
) {
    IMPORT_LAND(1, "Land", Regex("^NO$")),
    IMPORT_SEKTOR(2, "Sektor", Regex("^\\d$")),
    IMPORT_NARING(3, "Næring", Regex("^\\d{2}$")),
    IMPORT_NARINGSKODE(4, "Næringskode", Regex("^\\d{5}$")),
    IMPORT_BRANSJE(5, "Bransje", null),
    IMPORT_VIRKSOMHET(6, "Virksomhet", Regex("^\\d{9}$")),
    IMPORT_VIRKSOMHET_METADATA(7, "Virksomhet metadata", Regex("^\\d{9}$")),
    ;

    companion object {
        val iRekkefolge: List<ImportSteg> = entries.sortedBy { it.rekkefolge }
    }
}
