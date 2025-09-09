package no.nav.pia.dvhimport.helper

import ia.felles.definisjoner.bransjer.Bransje
import ia.felles.definisjoner.bransjer.BransjeId
import io.kotest.matchers.shouldBe
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.DatavarehusRecordType
import no.nav.pia.dvhimport.importjobb.ImportService.Companion.tilFilNavn
import no.nav.pia.dvhimport.importjobb.domene.DvhMetadata
import no.nav.pia.dvhimport.importjobb.domene.LandSykefraværsstatistikkDto
import no.nav.pia.dvhimport.importjobb.domene.StatistikkKategori
import no.nav.pia.dvhimport.importjobb.domene.SykefraværsstatistikkDto
import java.math.BigDecimal

class TestDataGenerator {
    companion object {
        const val TURBILTRANSPORT_NÆRINGSKODE = "49391"
        const val RUTEBILTRANSPORT_NÆRINGSKODE = "49392"

        fun lagTestDataForLand(
            land: String = "NO",
            årstall: Int = 2024,
            kvartal: Int = 2,
            prosent: BigDecimal = 6.2.toBigDecimal(),
            tapteDagsverk: BigDecimal = 8894426.768373.toBigDecimal(),
            muligeDagsverk: BigDecimal = 143458496.063556.toBigDecimal(),
            antallPersoner: Int = 3124427,
        ): LandSykefraværsstatistikkDto =
            LandSykefraværsstatistikkDto(
                land = land,
                årstall = årstall,
                kvartal = kvartal,
                prosent = prosent,
                tapteDagsverk = tapteDagsverk,
                muligeDagsverk = muligeDagsverk,
                antallPersoner = antallPersoner,
            )

        fun SykefraværsstatistikkDto.lagreITestBucket(
            gcsContainer: GoogleCloudStorageContainerHelper,
            kategori: StatistikkKategori,
            nøkkel: String,
            verdi: String,
        ) {
            val filnavn = tilFilNavn(kategori)
            gcsContainer.lagreTestBlob(
                blobNavn = filnavn,
                bytes =
                    """
                    [{
                      "årstall": ${this.årstall},
                      "kvartal": ${this.kvartal},
                      "$nøkkel": "$verdi",
                      "prosent": "${this.prosent}",
                      "tapteDagsverk": "${this.tapteDagsverk}",
                      "muligeDagsverk": "${this.muligeDagsverk}",
                      "antallPersoner": "${this.antallPersoner}"
                    }]
                    """.trimIndent().encodeToByteArray(),
            )

            val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
            verifiserBlobFinnes shouldBe true
        }

        fun lagTestDataForSektor(
            gcsContainer: GoogleCloudStorageContainerHelper,
            årstall: Int = 2024,
            kvartal: Int = 2,
        ) {
            val filnavn = tilFilNavn(StatistikkKategori.SEKTOR)
            gcsContainer.lagreTestBlob(
                blobNavn = filnavn,
                bytes =
                    """
                    [{
                      "årstall": $årstall,
                      "kvartal": $kvartal,
                      "sektor": "2",
                      "prosent": "6.2",
                      "tapteDagsverk": "88944.768373",
                      "muligeDagsverk": "1434584.063556",
                      "antallPersoner": "3124427"
                    },
                    {
                     "årstall": $årstall,
                     "kvartal": $kvartal,
                     "sektor": "3",
                     "prosent": "2.7",
                     "tapteDagsverk": "94426.768373",
                     "muligeDagsverk": "3458496.063556",
                     "antallPersoner": "24427"
                    }]
                    """.trimIndent().encodeToByteArray(),
            )

            val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
            verifiserBlobFinnes shouldBe true
        }

        fun lagTestDataForNæring(
            gcsContainer: GoogleCloudStorageContainerHelper,
            årstall: Int = 2024,
            kvartal: Int = 2,
            næring: String,
        ) {
            val filnavn = tilFilNavn(StatistikkKategori.NÆRING)
            gcsContainer.lagreTestBlob(
                blobNavn = filnavn,
                bytes =
                    """
                    [{
                      "årstall": $årstall,
                      "kvartal": $kvartal,
                      "næring": "$næring",
                      "prosent": "6.2",
                      "tapteDagsverk": "88944.768373",
                      "muligeDagsverk": "1434584.063556",
                      "tapteDagsverkGradert": 90.034285,
                      "tapteDagsverkPerVarighet": [
                        {
                          "varighet": "D",
                          "tapteDagsverk": 148.534285
                        }
                      ],
                      "antallPersoner": "3124427"
                    },
                    {
                     "årstall": $årstall,
                     "kvartal": $kvartal,
                     "næring": "88",
                     "prosent": "2.7",
                     "tapteDagsverk": "94426.768373",
                     "muligeDagsverk": "3458496.063556",
                     "tapteDagsverkGradert": 90.034285,
                     "tapteDagsverkPerVarighet": [
                       {
                         "varighet": "D",
                         "tapteDagsverk": 148.534285
                       }
                     ],
                     "antallPersoner": "24427"
                    }]
                    """.trimIndent().encodeToByteArray(),
            )
        }

        fun lagTestDataForNæringskode(
            gcsContainer: GoogleCloudStorageContainerHelper,
            årstall: Int = 2024,
            kvartal: Int = 2,
        ) {
            val filnavn = tilFilNavn(StatistikkKategori.NÆRINGSKODE)
            val næringskodeBarnehager = (Bransje.BARNEHAGER.bransjeId as BransjeId.Næringskoder).næringskoder.first()
            gcsContainer.lagreTestBlob(
                blobNavn = filnavn,
                bytes =
                    """
                    [{
                      "årstall": $årstall,
                      "kvartal": $kvartal,
                      "næringskode": "02300",
                      "prosent": "6.2",
                      "tapteDagsverk": "88944.768373",
                      "muligeDagsverk": "1434584.063556",
                      "tapteDagsverkGradert": 90.034285,
                      "tapteDagsverkPerVarighet": [
                        {
                          "varighet": "D",
                          "tapteDagsverk": 148.534285
                        }
                      ],
                      "antallPersoner": "3124427"
                    },
                    {
                     "årstall": $årstall,
                     "kvartal": $kvartal,
                     "næringskode": "$næringskodeBarnehager", 
                     "prosent": "2.7",
                     "tapteDagsverk": "94426.768373",
                     "muligeDagsverk": "3458496.063556",
                     "tapteDagsverkGradert": 90.034285,
                     "tapteDagsverkPerVarighet": [
                       {
                         "varighet": "D",
                         "tapteDagsverk": 148.534285
                       }
                     ],
                     "antallPersoner": "24427"
                    }]
                    """.trimIndent().encodeToByteArray(),
            )

            val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
            verifiserBlobFinnes shouldBe true
        }

        fun lagTestDataForNæringskodeSykehjem(
            gcsContainer: GoogleCloudStorageContainerHelper,
            årstall: Int = 2024,
            kvartal: Int = 2,
        ) {
            // Sykehjem: "87101" og "87102"
            // vi legger til "87103" for å teste at denne ikke blir plukket ut
            val filnavn = tilFilNavn(StatistikkKategori.NÆRINGSKODE)
            gcsContainer.lagreTestBlob(
                blobNavn = filnavn,
                bytes =
                    """
                    [{
                      "årstall": $årstall,
                      "kvartal": $kvartal,
                      "næringskode": "87101",
                      "prosent": 6.2,
                      "tapteDagsverk": 200.5,
                      "muligeDagsverk": 1010.3,
                      "tapteDagsverkGradert": 30.2,
                      "tapteDagsverkPerVarighet": [
                        {
                          "varighet": "D",
                          "tapteDagsverk": 8.66
                        }
                      ],
                      "antallPersoner": "399"
                    },
                    {
                     "årstall": $årstall,
                     "kvartal": $kvartal,
                     "næringskode": "87102",
                     "prosent": 3.5,
                     "tapteDagsverk": 100.5,
                     "muligeDagsverk": 2010.7,
                     "tapteDagsverkGradert": 70.8,
                     "tapteDagsverkPerVarighet": [
                       {
                         "varighet": "B",
                         "tapteDagsverk": 3.54444
                       },                     
                       {
                         "varighet": "D",
                         "tapteDagsverk": 10.44
                       }
                     ],
                     "antallPersoner": "201"
                    },
                    {
                     "årstall": $årstall,
                     "kvartal": $kvartal,
                     "næringskode": "87103",
                     "prosent": 23.5,
                     "tapteDagsverk": 23.5,
                     "muligeDagsverk": 100,
                     "tapteDagsverkGradert": 3,
                     "tapteDagsverkPerVarighet": [
                       {
                         "varighet": "C",
                         "tapteDagsverk": 8.88844
                       },                     
                       {
                         "varighet": "D",
                         "tapteDagsverk": 1000.11
                       },
                       {
                         "varighet": "E",
                         "tapteDagsverk": 11.41
                       }
                     ],
                     "antallPersoner": "999"
                    }]
                    """.trimIndent().encodeToByteArray(),
            )

            val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
            verifiserBlobFinnes shouldBe true
        }

        fun lagTestDataForVirksomhet(
            gcsContainer: GoogleCloudStorageContainerHelper,
            orgnr: String,
            årstall: Int,
            kvartal: Int,
            rectype: DatavarehusRecordType = DatavarehusRecordType.UNDERENHET,
        ) {
            val filnavn = tilFilNavn(StatistikkKategori.VIRKSOMHET)
            gcsContainer.lagreTestBlob(
                blobNavn = filnavn,
                bytes =
                    """
                    [{
                      "årstall": $årstall,
                      "kvartal": $kvartal,
                      "orgnr": "$orgnr",
                      "prosent": "26.0",
                      "tapteDagsverk": "20.23",
                      "muligeDagsverk": "77.8716",
                      "tapteDagsverkGradert": 90.034285,
                      "tapteDagsverkPerVarighet": [
                        {
                          "varighet": "A",
                          "tapteDagsverk": 12.1527
                        },
                        {
                          "varighet": "B",
                          "tapteDagsverk": 2.7
                        },
                        {
                          "varighet": "C",
                          "tapteDagsverk": 15
                        },
                        {
                          "varighet": "D",
                          "tapteDagsverk": 148.534285
                        },
                        {
                          "varighet": "E",
                          "tapteDagsverk": 142.6
                        },
                        {
                          "varighet": "F",
                          "tapteDagsverk": 31.4
                        }
                      ],
                      "antallPersoner": "40", 
                      "rectype": "${rectype.kode}"
                    }]
                    """.trimIndent().encodeToByteArray(),
            )

            val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
            verifiserBlobFinnes shouldBe true
        }

        fun lagTestDataForVirksomhetMetadata(
            gcsContainer: GoogleCloudStorageContainerHelper,
            årstall: Int = 2024,
            kvartal: Int = 2,
            primærnæring: String? = "88",
            primærnæringskode: String? = "88911",
        ) {
            val filnavn = tilFilNavn(DvhMetadata.VIRKSOMHET_METADATA)
            gcsContainer.lagreTestBlob(
                blobNavn = filnavn,
                bytes =
                    """
                    [{
                      "årstall": $årstall,
                      "kvartal": $kvartal,
                      "orgnr": "987654321",
                      "sektor": "2",
                      "primærnæring": ${nullOrStringWithQuotes(primærnæring)},
                      "primærnæringskode": ${nullOrStringWithQuotes(primærnæringskode)},
                      "rectype": "1"
                    }]
                    """.trimIndent().encodeToByteArray(),
            )

            val verifiserBlobFinnes = gcsContainer.verifiserBlobFinnes(blobNavn = filnavn)
            verifiserBlobFinnes shouldBe true
        }

        private fun nullOrStringWithQuotes(value: String?): String? = if (value != null) """"$value"""" else null
    }
}
