val gcsNioVersion = "0.127.26"
val googleCloudStorageVersion = "2.44.1"
val iaFellesVersion = "1.7.1"
val kafkaClientsVersion = "3.9.0"
val kotestVersion = "5.8.1"
val kotlinVersion = "2.0.21"
val ktorVersion = "3.0.1"
val logbackVersion = "1.5.12"
val logstashLogbackEncoderVersion = "8.0"
val prometeusVersion = "1.14.0"
val testcontainersVersion = "1.20.3"
val wiremockStandaloneVersion = "3.9.2"

plugins {
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "no.nav"

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("io.ktor:ktor-server-core-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-metrics-micrometer-jvm:$ktorVersion")
    implementation("io.micrometer:micrometer-registry-prometheus:$prometeusVersion")
    implementation("io.ktor:ktor-server-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-auth-jvm:$ktorVersion")
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-netty-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages-jvm:$ktorVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("net.logstash.logback:logstash-logback-encoder:$logstashLogbackEncoderVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.6.1")
    // Google Cloud Storage
    implementation("com.google.cloud:google-cloud-storage:${googleCloudStorageVersion}")
    // Kafka
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
    // Felles definisjoner for IA-domenet
    implementation("com.github.navikt:ia-felles:$iaFellesVersion")

    testImplementation("io.kotest:kotest-assertions-core:$kotestVersion")
    testImplementation("io.kotest:kotest-assertions-json:$kotestVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$kotlinVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:kafka:$testcontainersVersion")
    testImplementation("io.aiven:testcontainers-fake-gcs-server:0.2.0")
    testImplementation("org.wiremock:wiremock-standalone:$wiremockStandaloneVersion")
    // In-memory google cloud storage bucket
    testImplementation("com.google.cloud:google-cloud-nio:$gcsNioVersion")


    constraints {
        implementation("net.minidev:json-smart") {
            version {
                require("2.5.1")
            }
            because("From Kotlin version: 1.7.20 -> Earlier versions of json-smart package are vulnerable to Denial of Service (DoS) due to a StackOverflowError when parsing a deeply nested JSON array or object.")
        }
        implementation("io.netty:netty-codec-http2") {
            version {
                require("4.1.114.Final")
            }
            because("From Ktor version: 2.3.5 -> io.netty:netty-codec-http2 vulnerable to HTTP/2 Rapid Reset Attack")
        }
        testImplementation("org.apache.commons:commons-compress") {
            version {
                require("1.27.1")
            }
            because("testcontainers har sårbar versjon")
        }
        testImplementation("com.jayway.jsonpath:json-path") {
            version {
                require("2.9.0")
            }
            because(
                """
                json-path v2.8.0 was discovered to contain a stack overflow via the Criteria.parse() method.
                introdusert gjennom io.kotest:kotest-assertions-json:5.8.0
                """.trimIndent()
            )
        }
    }
}

tasks {
    shadowJar {
        manifest {
            attributes("Main-Class" to "no.nav.pia.dvhimport.ApplicationKt")
        }
    }
    test {
        dependsOn(shadowJar)
    }
}
