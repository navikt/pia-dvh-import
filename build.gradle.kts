val gcsNioVersion = "0.133.0"
val googleCloudStorageVersion = "2.69.0"
val iaFellesVersion = "1.10.2"
val kafkaClientsVersion = "4.3.0"
val kotestVersion = "6.1.11"
val kotlinVersion = "2.4.0"
val ktorVersion = "3.5.0"
val logbackVersion = "1.5.34"
val logstashLogbackEncoderVersion = "9.0"
val prometheusVersion = "1.16.5"
val testcontainersVersion = "2.0.5"
val wiremockStandaloneVersion = "3.13.2"
val opentelemetryLogbackMdcVersion = "2.28.1-alpha"

plugins {
    kotlin("jvm") version "2.4.0"
    kotlin("plugin.serialization") version "2.4.0"
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
    implementation("at.yawk.lz4:lz4-java:1.11.0")
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion") {
        // "Fikser CVE-2025-12183 - lz4-java >1.8.1 har sårbar versjon (transitive dependency fra kafka-clients:4.1.0)"
        exclude("org.lz4", "lz4-java")
    }
    // Felles definisjoner for IA-domenet
    implementation("com.github.navikt:ia-felles:$iaFellesVersion")
    // https://mvnrepository.com/artifact/io.opentelemetry.instrumentation/opentelemetry-logback-mdc-1.0

    testImplementation("io.kotest:kotest-assertions-core:$kotestVersion")
    testImplementation("io.kotest:kotest-assertions-json:$kotestVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$kotlinVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-kafka:$testcontainersVersion")
    testImplementation("io.aiven:testcontainers-fake-gcs-server:0.3.0")
    testImplementation("org.wiremock:wiremock-standalone:$wiremockStandaloneVersion")
    // In-memory google cloud storage bucket
    testImplementation("com.google.cloud:google-cloud-nio:$gcsNioVersion")

    constraints {
        implementation("com.fasterxml.jackson.core:jackson-core") {
            version { require("2.21.3") }
            because("versjoner < 2.21.1 har sårbarhet. inkludert i ktor-server-auth:3.4.0")
        }
        implementation("io.netty:netty-codec-http2") {
            version {
                require("4.2.13.Final")
            }
            because(
                "versjoner < 4.2.10.Final har sårbarhet. inkludert i ktor-server-netty-jvm:3.4.2",
            )
        }
        implementation("tools.jackson.core:jackson-core") {
            version { require("3.1.3") }
            because("versjoner < 3.1.0 har sårbarhet. inkludert i logstash-logback-encoder:9.0")
        }
    }

    tasks {
        test {
            dependsOn(installDist)
        }
    }
}
