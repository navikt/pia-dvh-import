package no.nav.pia.dvhimport.importjobb.kafka

import ia.felles.integrasjoner.jobbsender.Jobb
import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlin.test.Test

class SerializableJobbInfoUnitTest {
    @Test
    fun `JobbInfo kan inneholde årstall-og-kvartal og evt dry-run`() {
        kanParseÅrstallOgKvartal("2024-3")
        kanParseÅrstallOgKvartal("2024-3:DRY-RUN")

        shouldThrowMessage("Kunne ikke parse årstall og kvartal fra parameter: '2024K3'") {
            SerializableJobbInfo(
                jobb = Jobb.alleKategorierSykefraværsstatistikkDvhImport,
                tidspunkt = "2024-06-01T12:00:00Z",
                applikasjon = "pia-dvh-import",
                parameter = "2024K3",
            ).tilÅrstallOgKvartal()
        }
    }

    @Test
    fun `JobbInfo kan inneholde dry-run`() {
        jobbInfo("2024-3:DRY_RUN").tilDryRun() shouldBe true
        jobbInfo("DRY_RUN").tilDryRun() shouldBe true
        jobbInfo("").tilDryRun() shouldBe false // dry-run må bes om eksplisitt
        jobbInfo("2024-3").tilDryRun() shouldBe false
    }

    private fun kanParseÅrstallOgKvartal(jobbParameter: String) {
        val årstallOgKvartal = SerializableJobbInfo(
            jobb = Jobb.alleKategorierSykefraværsstatistikkDvhImport,
            tidspunkt = "2024-06-01T12:00:00Z",
            applikasjon = "pia-dvh-import",
            parameter = jobbParameter,
        ).tilÅrstallOgKvartal()

        årstallOgKvartal shouldNotBe null
        årstallOgKvartal!!.årstall shouldBe 2024
        årstallOgKvartal.kvartal shouldBe 3
    }

    private fun jobbInfo(jobbParameter: String) =
        SerializableJobbInfo(
            jobb = Jobb.alleKategorierSykefraværsstatistikkDvhImport,
            tidspunkt = "2024-06-01T12:00:00Z",
            applikasjon = "pia-dvh-import",
            parameter = jobbParameter,
        )
}
