val flywayPostgresqlVersion = "13.3.0"
val gcsNioVersion = "0.135.0"
val googleCloudStorageVersion = "2.71.0"
val hikariCPVersion = "7.1.0"
val iaFellesVersion = "3.1.1"
val kafkaClientsVersion = "4.3.1"
val kotliqueryVersion = "1.9.1"
val kotestVersion = "6.2.4"
val kotlinVersion = "2.4.10"
val ktorVersion = "3.5.2"
val logbackVersion = "1.6.3"
val logstashLogbackEncoderVersion = "9.0"
val mockServerVersion = "2.51.1"
val postgresqlVersion = "42.7.13"
val prometheusVersion = "1.17.0"
val testcontainersVersion = "2.0.5"
val wiremockStandaloneVersion = "3.13.2"
val opentelemetryLogbackMdcVersion = "2.30.0-alpha"

plugins {
    kotlin("jvm") version "2.4.10"
    kotlin("plugin.serialization") version "2.4.10"
    id("application")
}

group = "no.nav"

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("io.ktor:ktor-server-core-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-metrics-micrometer-jvm:$ktorVersion")
    implementation("io.micrometer:micrometer-registry-prometheus:$prometheusVersion")
    implementation("io.ktor:ktor-server-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-auth-jvm:$ktorVersion")
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-netty-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages-jvm:$ktorVersion")

    // Logger
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("net.logstash.logback:logstash-logback-encoder:$logstashLogbackEncoderVersion")
    implementation("io.opentelemetry.instrumentation:opentelemetry-logback-mdc-1.0:$opentelemetryLogbackMdcVersion")

    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.8.0-0.6.x-compat")
    // Google Cloud Storage
    implementation("com.google.cloud:google-cloud-storage:$googleCloudStorageVersion")
    // Kafka
    implementation("at.yawk.lz4:lz4-java:1.11.2")
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion") {
        // "Fikser CVE-2025-12183 - lz4-java >1.8.1 har sårbar versjon (transitive dependency fra kafka-clients:4.1.0)"
        exclude("org.lz4", "lz4-java")
    }
    // Felles definisjoner for IA-domenet
    implementation("com.github.navikt:ia-felles:$iaFellesVersion")

    // Database
    implementation("org.postgresql:postgresql:$postgresqlVersion")
    implementation("com.zaxxer:HikariCP:$hikariCPVersion")
    implementation("org.flywaydb:flyway-database-postgresql:$flywayPostgresqlVersion")
    implementation("com.github.seratch:kotliquery:$kotliqueryVersion")
    // https://mvnrepository.com/artifact/io.opentelemetry.instrumentation/opentelemetry-logback-mdc-1.0

    testImplementation("io.kotest:kotest-assertions-core:$kotestVersion")
    testImplementation("io.kotest:kotest-assertions-json:$kotestVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$kotlinVersion")
    testImplementation("io.mockk:mockk:1.14.11")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-kafka:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-postgresql:$testcontainersVersion")
    testImplementation("io.aiven:testcontainers-fake-gcs-server:0.3.0")
    testImplementation("org.wiremock:wiremock-standalone:$wiremockStandaloneVersion")
    // In-memory Google Cloud storage bucket
    testImplementation("com.google.cloud:google-cloud-nio:$gcsNioVersion")
    // Mockserver neolight
    testImplementation("software.xdev.mockserver:testcontainers:$mockServerVersion")
    testImplementation("software.xdev.mockserver:client:$mockServerVersion")

    constraints {
        implementation("com.fasterxml.jackson.core:jackson-core") {
            version { require("2.22.1") }
            because("versjoner < 2.22.1 har sårbarhet. inkludert i ktor-server-auth:3.4.0")
        }
        implementation("com.fasterxml.jackson.core:jackson-databind") {
            version { require("2.22.1") }
            because("versjoner < 2.22.1 har sårbarhet. inkludert i ktor-server-auth:3.4.0")
        }
        implementation("io.netty:netty-codec-http2") {
            version {
                require("4.2.16.Final")
            }
            because(
                "versjoner < 4.2.16.Final har sårbarhet. inkludert i ktor-server-netty-jvm:3.4.2",
            )
        }
        implementation("tools.jackson.core:jackson-core") {
            version { require("3.2.1") }
            because("versjoner < 3.1.0 har sårbarhet. inkludert i logstash-logback-encoder:9.0")
        }
    }

    tasks {
        test {
            dependsOn(installDist)
        }
    }
}
