package no.nav.pia.dvhimport.storage

import no.nav.pia.dvhimport.importjobb.domene.ÅrstallOgKvartal

/*
  Denne klassen beskriver mappestruktur i GCP bucket
   - mappe 'år' (f.eks: '2024' -- kan være tom eller inneholde sistePubliserteKvartal)
   - mappe 'sistePubliserteKvartal' (f.eks: 'K2' -- slettet 30 dager etter publisering)
* */
class Mappestruktur(
    val publiseringsÅr: String,
    val sistePubliserteKvartal: String,
    val brukÅrOgKvartalIPathTilFilene: Boolean = true,
) {
    companion object {
        fun ÅrstallOgKvartal.tilMappestruktur(brukÅrOgKvartalIPathTilFilene: Boolean) =
            Mappestruktur(
                publiseringsÅr = "$årstall",
                sistePubliserteKvartal = "K$kvartal",
                brukÅrOgKvartalIPathTilFilene = brukÅrOgKvartalIPathTilFilene,
            )
    }

    fun gjeldendeÅrstallOgKvartal(): ÅrstallOgKvartal =
        ÅrstallOgKvartal(
            årstall = publiseringsÅr.toInt(),
            kvartal = sistePubliserteKvartal.subSequence(startIndex = 1, endIndex = 2).toString().toInt(),
        )

    fun pathTilKvartalsvisData() = if (brukÅrOgKvartalIPathTilFilene) "${this.publiseringsÅr}/${this.sistePubliserteKvartal}" else ""

    fun pathTilÅrsvisData() = if (brukÅrOgKvartalIPathTilFilene) this.publiseringsÅr else ""
}
