package no.nav.pia.dvhimport.importjobb.publiseringsdato

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.time.LocalDate
import javax.sql.DataSource

class PubliseringsdatoRepository(private val dataSource: DataSource) {

    fun hentUprosesserteForDato(dato: LocalDate): List<PubliseringsdatoDto> =
        using(sessionOf(dataSource)) { session ->
            session.list(
                queryOf(
                    """
                    SELECT id, arstall, kvartal, dato, prosessert
                    FROM publiseringsdato
                    WHERE dato = :dato AND prosessert = false
                    """.trimIndent(),
                    mapOf("dato" to dato),
                ),
            ) { row ->
                PubliseringsdatoDto(
                    id = row.int("id"),
                    årstall = row.int("arstall"),
                    kvartal = row.int("kvartal"),
                    dato = row.localDate("dato"),
                    prosessert = row.boolean("prosessert"),
                )
            }
        }

    fun markerSomProsessert(id: Int) {
        using(sessionOf(dataSource)) { session ->
            session.run(
                queryOf(
                    """
                    UPDATE publiseringsdato
                    SET prosessert = true
                    WHERE id = :id
                    """.trimIndent(),
                    mapOf("id" to id),
                ).asUpdate,
            )
        }
    }

    fun lagrePubliseringsdato(årstall: Int, kvartal: Int, dato: LocalDate): LagreResultat =
        using(sessionOf(dataSource)) { session ->
            session.single(
                queryOf(
                    """
                    INSERT INTO publiseringsdato (arstall, kvartal, dato)
                    VALUES (:arstall, :kvartal, :dato)
                    ON CONFLICT (arstall, kvartal) DO UPDATE SET
                        dato = EXCLUDED.dato,
                        prosessert = false
                    WHERE publiseringsdato.dato != EXCLUDED.dato
                    RETURNING (xmax = 0) AS er_ny
                    """.trimIndent(),
                    mapOf("arstall" to årstall, "kvartal" to kvartal, "dato" to dato),
                ),
            ) { row ->
                if (row.boolean("er_ny")) LagreResultat.NY else LagreResultat.OPPDATERT
            } ?: LagreResultat.UENDRET
        }
}
