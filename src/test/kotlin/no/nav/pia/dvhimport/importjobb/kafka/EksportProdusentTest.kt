package no.nav.pia.dvhimport.importjobb.kafka

import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.datetime.LocalDateTime
import no.nav.pia.dvhimport.importjobb.publiseringsdato.PubliseringsdatoKafkaDto
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.KafkaSendException
import no.nav.pia.dvhimport.importjobb.kafka.EksportProdusent.PubliseringsdatoMelding
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.RoundRobinPartitioner
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.test.Test

class EksportProdusentTest {

    private fun lagMockProducer(autoComplete: Boolean = true) =
        MockProducer(autoComplete, RoundRobinPartitioner(), StringSerializer(), StringSerializer())

    private fun lagTestMelding() = PubliseringsdatoMelding(
        årstall = 2025,
        kvartal = 1,
        publiseringsdato = PubliseringsdatoKafkaDto(
            rapportPeriode = "202501",
            offentligDato = LocalDateTime(2025, 5, 28, 8, 0),
            oppdatertIDvh = LocalDateTime(2025, 1, 15, 10, 0),
        ),
    )

    @Test
    fun `flushOgSjekkFeil kaster ikke exception når sending er vellykket`() {
        val mockProducer = lagMockProducer(autoComplete = true)
        val produsent = EksportProdusent(kafkaProducer = mockProducer)

        produsent.sendMelding(lagTestMelding())

        shouldNotThrowAny {
            produsent.flushOgSjekkFeil()
        }
        mockProducer.history().size shouldBe 1
    }

    @Test
    fun `flushOgSjekkFeil kaster KafkaSendException ved TopicAuthorizationException`() {
        val mockProducer = lagMockProducer(autoComplete = false)
        val produsent = EksportProdusent(kafkaProducer = mockProducer)

        produsent.sendMelding(lagTestMelding())
        mockProducer.errorNext(TopicAuthorizationException("Ikke tilgang til topic"))

        val exception = shouldThrow<KafkaSendException> {
            produsent.flushOgSjekkFeil()
        }
        exception.message shouldContain "Kafka-sending feilet"
        exception.cause shouldBe TopicAuthorizationException::class.java.let { exception.cause }
    }

    @Test
    fun `flushOgSjekkFeil fanger første feil når flere meldinger feiler`() {
        val mockProducer = lagMockProducer(autoComplete = false)
        val produsent = EksportProdusent(kafkaProducer = mockProducer)

        produsent.sendMelding(lagTestMelding())
        produsent.sendMelding(lagTestMelding())

        mockProducer.errorNext(TopicAuthorizationException("Feil 1"))
        mockProducer.errorNext(RuntimeException("Feil 2"))

        val exception = shouldThrow<KafkaSendException> {
            produsent.flushOgSjekkFeil()
        }
        exception.cause!!.message shouldContain "Feil 1"
    }

    @Test
    fun `flushOgSjekkFeil resetter feil etter kast slik at neste flush er OK`() {
        val mockProducer = lagMockProducer(autoComplete = false)
        val produsent = EksportProdusent(kafkaProducer = mockProducer)

        produsent.sendMelding(lagTestMelding())
        mockProducer.errorNext(TopicAuthorizationException("Feil"))

        shouldThrow<KafkaSendException> {
            produsent.flushOgSjekkFeil()
        }

        mockProducer.clear()

        produsent.sendMelding(lagTestMelding())
        mockProducer.completeNext()

        shouldNotThrowAny {
            produsent.flushOgSjekkFeil()
        }
    }
}
