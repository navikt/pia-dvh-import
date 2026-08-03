package no.nav.pia.dvhimport.importjobb.domene

import ia.felles.integrasjoner.jobbsender.Jobb
import no.nav.pia.dvhimport.helper.KonsistentTestdata
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.varsling.SlackVarsler.SlackMelding
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class SlackVarslerIntegrasjonstest {
    private val gcsContainer = TestContainerHelper.googleCloudStorage
    private val kafkaContainer = TestContainerHelper.kafka
    private val slackWebhookContainerHelper = TestContainerHelper.slackWebhookContainerHelper

    @BeforeTest
    fun setup() {
        gcsContainer.opprettTestBucketHvisIkkeFunnet()
        slackWebhookContainerHelper.slettAlleMeldinger()
    }

    @AfterTest
    fun tearDown() {
        slackWebhookContainerHelper.slettAlleMeldinger()
    }

    @Test
    fun `import statistikk for alle kategorier`() {
        KonsistentTestdata.skrivAlleKonsistenteFiler(gcsContainer = gcsContainer, årstall = 2026, kvartal = 2)

        kafkaContainer.sendJobbMelding(Jobb.alleKategorierSykefraværsstatistikkDvhImport, "2026-2")

        slackWebhookContainerHelper.shouldHaveReceived(
            meldinger = listOf(
                SlackMelding(text = "📥 Import startet for 2. kvartal 2026"),
                SlackMelding(text = "✅ Alle kategorier validert for 2. kvartal 2026 — starter sending"),
                SlackMelding(text = "✅ Land ferdig"),
                SlackMelding(text = "✅ Sektor ferdig"),
                SlackMelding(text = "✅ Næring ferdig"),
                SlackMelding(text = "✅ Næringskode ferdig"),
                SlackMelding(text = "✅ Bransje ferdig"),
                SlackMelding(text = "✅ Virksomhet ferdig"),
                SlackMelding(text = "✅ Virksomhet metadata ferdig"),
                SlackMelding(text = "🎉 Import ferdig for 2. kvartal 2026"),
            ),
        )
    }
}
