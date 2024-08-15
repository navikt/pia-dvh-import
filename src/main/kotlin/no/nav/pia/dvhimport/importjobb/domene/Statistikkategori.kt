package no.nav.pia.dvhimport.importjobb.domene

enum class Statistikkategori : DvhDatakilde {
    LAND {
        override fun tilFilnavn(): String {
            return "land.json"
        }
    },
    SEKTOR {
        override fun tilFilnavn(): String {
            return "sektor.json"
        }
    },
    NÆRING {
        override fun tilFilnavn(): String {
            return "naering.json"
        }
    },
    NÆRINGSKODE {
        override fun tilFilnavn(): String {
            return "naeringskode.json"
        }
    },
    VIRKSOMHET {
        override fun tilFilnavn(): String {
            return "virksomhet.json"
        }
    },
    VIRKSOMHET_METADATA {
        override fun tilFilnavn(): String {
            return "virksomhet_metadata.json"
        }
    },
}

enum class Metadata : DvhDatakilde {
    PUBLISERINGSDATO {
        override fun tilFilnavn(): String {
            return "publiseringsdato.json"
        }
    },
}

interface DvhDatakilde {
    fun tilFilnavn(): String
}

