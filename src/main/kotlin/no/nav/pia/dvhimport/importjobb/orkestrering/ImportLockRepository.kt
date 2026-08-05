package no.nav.pia.dvhimport.importjobb.orkestrering

import kotliquery.Row
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import javax.sql.DataSource

class ImportLockRepository(
    private val dataSource: DataSource,
) {
    fun hentForPubliseringsdato(publiseringsdatoId: Int): ImportLockDto? =
        using(sessionOf(dataSource)) { session ->
            session.single(
                queryOf(
                    """
                    SELECT id, publiseringsdato_id, status, start_dato, slutt_dato
                    FROM automatisering_import_lock
                    WHERE publiseringsdato_id = :publiseringsdatoId
                    """.trimIndent(),
                    mapOf("publiseringsdatoId" to publiseringsdatoId),
                ),
                ::tilImportLockDto,
            )
        }

    fun taLås(publiseringsdatoId: Int): ImportLockDto? =
        using(sessionOf(dataSource)) { session ->
            session.single(
                queryOf(
                    """
                    INSERT INTO automatisering_import_lock (publiseringsdato_id, status)
                    VALUES (:publiseringsdatoId, :status)
                    ON CONFLICT (publiseringsdato_id) DO NOTHING
                    RETURNING id, publiseringsdato_id, status, start_dato, slutt_dato
                    """.trimIndent(),
                    mapOf(
                        "publiseringsdatoId" to publiseringsdatoId,
                        "status" to ImportLockStatus.STARTET.name,
                    ),
                ),
                ::tilImportLockDto,
            )
        }

    fun markerStartet(publiseringsdatoId: Int) = oppdaterStatus(publiseringsdatoId, ImportLockStatus.STARTET, settSluttDato = false)

    fun markerFeilet(publiseringsdatoId: Int) = oppdaterStatus(publiseringsdatoId, ImportLockStatus.FEILET, settSluttDato = false)

    fun markerFerdig(publiseringsdatoId: Int) = oppdaterStatus(publiseringsdatoId, ImportLockStatus.FERDIG, settSluttDato = true)

    private fun oppdaterStatus(
        publiseringsdatoId: Int,
        status: ImportLockStatus,
        settSluttDato: Boolean,
    ) {
        using(sessionOf(dataSource)) { session ->
            session.run(
                queryOf(
                    """
                    UPDATE automatisering_import_lock
                    SET status = :status,
                        slutt_dato = ${if (settSluttDato) "now()" else "slutt_dato"}
                    WHERE publiseringsdato_id = :publiseringsdatoId
                    """.trimIndent(),
                    mapOf(
                        "status" to status.name,
                        "publiseringsdatoId" to publiseringsdatoId,
                    ),
                ).asUpdate,
            )
        }
    }

    private fun tilImportLockDto(row: Row): ImportLockDto =
        ImportLockDto(
            id = row.int("id"),
            publiseringsdatoId = row.int("publiseringsdato_id"),
            status = ImportLockStatus.valueOf(row.string("status")),
            startDato = row.localDateTime("start_dato"),
            sluttDato = row.localDateTimeOrNull("slutt_dato"),
        )
}
