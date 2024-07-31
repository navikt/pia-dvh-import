package no.nav.pia.dvhimport.importjobb

class Feil(
    val feilmelding: String? = null,
    val opprinneligException: Throwable? = null,
): Throwable(feilmelding, opprinneligException)