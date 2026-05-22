package no.nav.pia.dvhimport.importjobb.publiseringsdato

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.time.LocalDate
import javax.sql.DataSource

class PubliseringsdatoRepository(private val dataSource: DataSource) {

    fun hentUprosessertForDato(dato: LocalDate): PubliseringsdatoDto? =
        using(sessionOf(dataSource)) { session ->
            session.single(
                queryOf(
                    """
                    SELECT id, arstall, kvartal, offentlig_dato, prosessert
                    FROM publiseringsdato
                    WHERE offentlig_dato = :dato AND prosessert = false
                    """.trimIndent(),
                    mapOf("dato" to dato),
                ),
            ) { row ->
                PubliseringsdatoDto(
                    id = row.int("id"),
                    årstall = row.int("arstall"),
                    kvartal = row.int("kvartal"),
                    dato = row.localDate("offentlig_dato"),
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
                    INSERT INTO publiseringsdato (arstall, kvartal, offentlig_dato, sist_endret)
                    VALUES (:arstall, :kvartal, :dato, NULL)
                    ON CONFLICT (arstall, kvartal) DO UPDATE SET
                        offentlig_dato = EXCLUDED.offentlig_dato,
                        prosessert = false,
                        sist_endret = now()
                    WHERE publiseringsdato.offentlig_dato != EXCLUDED.offentlig_dato
                    RETURNING (sist_endret IS NULL) AS er_ny
                    """.trimIndent(),
                    mapOf("arstall" to årstall, "kvartal" to kvartal, "dato" to dato),
                ),
            ) { row ->
                if (row.boolean("er_ny")) LagreResultat.NY else LagreResultat.OPPDATERT
            } ?: LagreResultat.UENDRET
        }
}
