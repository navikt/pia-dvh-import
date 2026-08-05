package no.nav.pia.dvhimport.importjobb.orkestrering

class ValideringsfeilException(
    val kontroll: Kontroll,
    melding: String,
) : Exception(melding)
