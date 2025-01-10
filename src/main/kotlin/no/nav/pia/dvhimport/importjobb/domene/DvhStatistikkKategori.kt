package no.nav.pia.dvhimport.importjobb.domene

enum class DvhStatistikkKategori : DvhDatakilde {
    LAND {
        override fun tilFilnavn(): String = "land.json"
    },
    SEKTOR {
        override fun tilFilnavn(): String = "sektor.json"
    },
    NÆRING {
        override fun tilFilnavn(): String = "naering.json"
    },
    NÆRINGSKODE {
        override fun tilFilnavn(): String = "naeringskode.json"
    },
    VIRKSOMHET {
        override fun tilFilnavn(): String = "virksomhet.json"
    },

    @Deprecated("bruk DvhMetadata.VIRKSOMHET_METADATA")
    VIRKSOMHET_METADATA {
        override fun tilFilnavn(): String = "virksomhet_metadata.json"
    },
}

enum class DvhMetadata : DvhDatakilde {
    PUBLISERINGSDATO {
        override fun tilFilnavn(): String = "publiseringsdato.json"
    },
    VIRKSOMHET_METADATA {
        override fun tilFilnavn(): String = "virksomhet_metadata.json"
    },
}

interface DvhDatakilde {
    fun tilFilnavn(): String
}
