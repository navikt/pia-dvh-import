package no.nav.pia.dvhimport.importjobb.orkestrering

import kotliquery.Row
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.math.BigDecimal
import javax.sql.DataSource

class ImportStegRepository(
    private val dataSource: DataSource,
) {
    fun opprettStegHvisIkkeFinnes(publiseringsdatoId: Int) {
        using(sessionOf(dataSource)) { session ->
            ImportSteg.iRekkefolge.forEach { steg ->
                session.run(
                    queryOf(
                        """
                        INSERT INTO automatisering_import_steg (publiseringsdato_id, navn, rekkefolge, status)
                        VALUES (:publiseringsdatoId, :navn, :rekkefolge, :status)
                        ON CONFLICT (publiseringsdato_id, navn) DO NOTHING
                        """.trimIndent(),
                        mapOf(
                            "publiseringsdatoId" to publiseringsdatoId,
                            "navn" to steg.name,
                            "rekkefolge" to steg.rekkefolge,
                            "status" to ImportStegStatus.PLANLAGT.name,
                        ),
                    ).asUpdate,
                )
            }
        }
    }

    fun hentAlle(publiseringsdatoId: Int): List<ImportStegDto> =
        using(sessionOf(dataSource)) { session ->
            session.list(
                queryOf(
                    """
                    SELECT id, publiseringsdato_id, navn, rekkefolge, status, kontroll,
                           start_dato, slutt_dato, antall_rader_lest, antall_sendt_paa_kafka, sf_prosent
                    FROM automatisering_import_steg
                    WHERE publiseringsdato_id = :publiseringsdatoId
                    ORDER BY rekkefolge
                    """.trimIndent(),
                    mapOf("publiseringsdatoId" to publiseringsdatoId),
                ),
                ::tilImportStegDto,
            )
        }

    fun hent(
        publiseringsdatoId: Int,
        steg: ImportSteg,
    ): ImportStegDto? =
        using(sessionOf(dataSource)) { session ->
            session.single(
                queryOf(
                    """
                    SELECT id, publiseringsdato_id, navn, rekkefolge, status, kontroll,
                           start_dato, slutt_dato, antall_rader_lest, antall_sendt_paa_kafka, sf_prosent
                    FROM automatisering_import_steg
                    WHERE publiseringsdato_id = :publiseringsdatoId AND navn = :navn
                    """.trimIndent(),
                    mapOf("publiseringsdatoId" to publiseringsdatoId, "navn" to steg.name),
                ),
                ::tilImportStegDto,
            )
        }

    fun markerStartet(
        publiseringsdatoId: Int,
        steg: ImportSteg,
    ) {
        using(sessionOf(dataSource)) { session ->
            session.run(
                queryOf(
                    """
                    UPDATE automatisering_import_steg
                    SET status = :status, start_dato = now(), kontroll = NULL, slutt_dato = NULL
                    WHERE publiseringsdato_id = :publiseringsdatoId AND navn = :navn
                    """.trimIndent(),
                    mapOf(
                        "status" to ImportStegStatus.STARTET.name,
                        "publiseringsdatoId" to publiseringsdatoId,
                        "navn" to steg.name,
                    ),
                ).asUpdate,
            )
        }
    }

    fun markerValidert(
        publiseringsdatoId: Int,
        steg: ImportSteg,
        antallRaderLest: Int,
        sfProsent: BigDecimal?,
    ) {
        using(sessionOf(dataSource)) { session ->
            session.run(
                queryOf(
                    """
                    UPDATE automatisering_import_steg
                    SET status = :status, antall_rader_lest = :antallRaderLest, sf_prosent = :sfProsent
                    WHERE publiseringsdato_id = :publiseringsdatoId AND navn = :navn
                    """.trimIndent(),
                    mapOf(
                        "status" to ImportStegStatus.VALIDERT.name,
                        "antallRaderLest" to antallRaderLest,
                        "sfProsent" to sfProsent,
                        "publiseringsdatoId" to publiseringsdatoId,
                        "navn" to steg.name,
                    ),
                ).asUpdate,
            )
        }
    }

    fun markerFerdig(
        publiseringsdatoId: Int,
        steg: ImportSteg,
        antallSendtPaaKafka: Int,
    ) {
        using(sessionOf(dataSource)) { session ->
            session.run(
                queryOf(
                    """
                    UPDATE automatisering_import_steg
                    SET status = :status, kontroll = :kontroll, slutt_dato = now(),
                        antall_sendt_paa_kafka = :antallSendtPaaKafka
                    WHERE publiseringsdato_id = :publiseringsdatoId AND navn = :navn
                    """.trimIndent(),
                    mapOf(
                        "status" to ImportStegStatus.FERDIG.name,
                        "kontroll" to Kontroll.OK.name,
                        "antallSendtPaaKafka" to antallSendtPaaKafka,
                        "publiseringsdatoId" to publiseringsdatoId,
                        "navn" to steg.name,
                    ),
                ).asUpdate,
            )
        }
    }

    fun markerFeilet(
        publiseringsdatoId: Int,
        steg: ImportSteg,
        kontroll: Kontroll,
    ) {
        using(sessionOf(dataSource)) { session ->
            session.run(
                queryOf(
                    """
                    UPDATE automatisering_import_steg
                    SET status = :status, kontroll = :kontroll, slutt_dato = now()
                    WHERE publiseringsdato_id = :publiseringsdatoId AND navn = :navn
                    """.trimIndent(),
                    mapOf(
                        "status" to ImportStegStatus.FEILET.name,
                        "kontroll" to kontroll.name,
                        "publiseringsdatoId" to publiseringsdatoId,
                        "navn" to steg.name,
                    ),
                ).asUpdate,
            )
        }
    }

    private fun tilImportStegDto(row: Row): ImportStegDto =
        ImportStegDto(
            id = row.int("id"),
            publiseringsdatoId = row.int("publiseringsdato_id"),
            steg = ImportSteg.valueOf(row.string("navn")),
            rekkefolge = row.int("rekkefolge"),
            status = ImportStegStatus.valueOf(row.string("status")),
            kontroll = row.stringOrNull("kontroll")?.let { Kontroll.valueOf(it) },
            startDato = row.localDateTimeOrNull("start_dato"),
            sluttDato = row.localDateTimeOrNull("slutt_dato"),
            antallRaderLest = row.int("antall_rader_lest"),
            antallSendtPaaKafka = row.int("antall_sendt_paa_kafka"),
            sfProsent = row.bigDecimalOrNull("sf_prosent"),
        )
}
