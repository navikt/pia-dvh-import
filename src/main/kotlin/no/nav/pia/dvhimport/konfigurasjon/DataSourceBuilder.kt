package no.nav.pia.dvhimport.konfigurasjon

import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import javax.sql.DataSource

fun createDataSource(jdbcUrl: String): DataSource =
    HikariDataSource().apply {
        this.jdbcUrl = jdbcUrl
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 100000
        connectionTimeout = 100000
        maxLifetime = 300000
    }

fun runMigration(dataSource: DataSource) {
    Flyway.configure()
        .validateMigrationNaming(true)
        .dataSource(dataSource)
        .load()
        .migrate()
}
