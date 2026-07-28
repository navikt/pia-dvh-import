package no.nav.pia.dvhimport.konfigurasjon

import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import javax.sql.DataSource

fun createDataSource(jdbcUrl: String): DataSource =
    HikariDataSource().apply {
        this.jdbcUrl = jdbcUrl
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 300_000 // 5 min
        connectionTimeout = 30_000 // 30 sek
        maxLifetime = 1_800_000 // 30 min
    }

fun runMigration(dataSource: DataSource) {
    Flyway.configure()
        .validateMigrationNaming(true)
        .dataSource(dataSource)
        .load()
        .migrate()
}
