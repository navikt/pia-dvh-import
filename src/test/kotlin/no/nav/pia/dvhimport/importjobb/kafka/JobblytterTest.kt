package no.nav.pia.dvhimport.importjobb.kafka

import ia.felles.integrasjoner.jobbsender.Jobb.iaSakEksport
import ia.felles.integrasjoner.jobbsender.Jobb.importSykefraværKvartalsstatistikk
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import kotlin.test.Test


class JobblytterTest {
    private val kafkaContainer = TestContainerHelper.kafka

    @Test
    fun `skal kunne trigge import jobb via kafka`() {
        kafkaContainer.sendJobbMelding(importSykefraværKvartalsstatistikk)
        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for alle statistikkkategorier".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'importSykefraværKvartalsstatistikk' ferdig".toRegex()
    }

    @Test
    fun `skal ignorere jobber applikasjon ikke kjenner`() {
        val jobbSomIkkeSkalKjøreIDvhImportApplikasjon = iaSakEksport
        kafkaContainer.sendJobbMelding(jobbSomIkkeSkalKjøreIDvhImportApplikasjon)
        dvhImportApplikasjon shouldContainLog "Jobb '$jobbSomIkkeSkalKjøreIDvhImportApplikasjon' ignorert".toRegex()
    }
}