package no.nav.pia.dvhimport.helper

import org.slf4j.Logger
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import org.testcontainers.containers.PostgreSQLContainer

class PostgresContainerHelper(
    network: Network,
    log: Logger,
) {
    private val postgresNetworkAlias = "postgrescontainer"
    private val dbName = "pia-dvh-import-db"
    val container: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:17")
            .withNetwork(network)
        .withDatabaseName(dbName)
        .waitingFor(HostPortWaitStrategy())
        .withNetworkAliases(postgresNetworkAlias)
        .withCreateContainerCmdModifier { cmd -> cmd.withName("$postgresNetworkAlias-${System.currentTimeMillis()}") }
        .withLogConsumer(
            Slf4jLogConsumer(log)
                .withPrefix(postgresNetworkAlias)
                .withSeparateOutputStreams(),
        )
        .apply { start() }

    fun envVars() =
        mapOf(
            "NAIS_DATABASE_PIA_DVH_IMPORT_PIA_DVH_IMPORT_DB_JDBC_URL" to
                "jdbc:postgresql://$postgresNetworkAlias:5432/$dbName?user=${container.username}&password=${container.password}",
        )
}
