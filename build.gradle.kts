val gcsNioVersion = "0.128.7"
val googleCloudStorageVersion = "2.59.0"
val iaFellesVersion = "1.10.2"
val kafkaClientsVersion = "4.1.0"
val kotestVersion = "6.0.4"
val kotlinVersion = "2.2.21"
val ktorVersion = "3.3.2"
val logbackVersion = "1.5.20"
val logstashLogbackEncoderVersion = "9.0"
val prometheusVersion = "1.15.5"
val testcontainersVersion = "2.0.1"
val wiremockStandaloneVersion = "3.13.1"
val opentelemetryLogbackMdcVersion = "2.16.0-alpha"

plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
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

    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.7.1-0.6.x-compat")
    // Google Cloud Storage
    implementation("com.google.cloud:google-cloud-storage:$googleCloudStorageVersion")
    // Kafka
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
    // Felles definisjoner for IA-domenet
    implementation("com.github.navikt:ia-felles:$iaFellesVersion")
    // https://mvnrepository.com/artifact/io.opentelemetry.instrumentation/opentelemetry-logback-mdc-1.0

    testImplementation("io.kotest:kotest-assertions-core:$kotestVersion")
    testImplementation("io.kotest:kotest-assertions-json:$kotestVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$kotlinVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-kafka:$testcontainersVersion")
    testImplementation("io.aiven:testcontainers-fake-gcs-server:0.2.0")
    testImplementation("org.wiremock:wiremock-standalone:$wiremockStandaloneVersion")
    // In-memory google cloud storage bucket
    testImplementation("com.google.cloud:google-cloud-nio:$gcsNioVersion")

    constraints {
        implementation("net.minidev:json-smart") {
            version {
                require("2.6.0")
            }
            because(
                "Kotest 6.0.0.M4 inneholder sårbarversjon 2.5.0",
            )
        }
    }
}
