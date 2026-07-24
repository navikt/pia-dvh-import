package no.nav.pia.dvhimport.importjobb.orkestrering

enum class ImportLockStatus {
    STARTET,
    FEILET,
    FERDIG,
}

enum class ImportStegStatus {
    PLANLAGT,
    STARTET,
    VALIDERT,
    FEILET,
    FERDIG,
}

enum class Kontroll {
    OK,
    SF_PROSENT_FEIL,
    FEIL_ANTALL_RADER_I_INPUT_FIL,
    INPUT_FIL_IKKE_FUNNET,
    FEIL_ÅRSTALL_ELLER_KVARTAL,
    FEIL_STRUKTUR_I_INPUT_FIL,
    KAFKA_ERROR,
    ANNET,
}
