package no.nav.pia.dvhimport.importjobb.orkestrering

enum class ImportSteg(
    val rekkefolge: Int,
    val strukturRegex: Regex?,
) {
    IMPORT_LAND(1, Regex("^NO$")),
    IMPORT_SEKTOR(2, Regex("^\\d$")),
    IMPORT_NARING(3, Regex("^\\d{2}$")),
    IMPORT_NARINGSKODE(4, Regex("^\\d{5}$")),
    IMPORT_BRANSJE(5, null),
    IMPORT_VIRKSOMHET(6, Regex("^\\d{9}$")),
    IMPORT_VIRKSOMHET_METADATA(7, Regex("^\\d{9}$")),
    ;

    companion object {
        val iRekkefolge: List<ImportSteg> = entries.sortedBy { it.rekkefolge }
    }
}
