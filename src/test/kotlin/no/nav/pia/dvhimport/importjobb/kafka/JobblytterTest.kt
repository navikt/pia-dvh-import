package no.nav.pia.dvhimport.importjobb.kafka

import ia.felles.integrasjoner.jobbsender.Jobb.alleKategorierSykefraværsstatistikkDvhImport
import ia.felles.integrasjoner.jobbsender.Jobb.iaSakEksport
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import kotlin.test.Test


class JobblytterTest {
    private val kafkaContainer = TestContainerHelper.kafka

    @Test
    fun `skal kunne trigge import jobb via kafka`() {
        kafkaContainer.sendJobbMelding(alleKategorierSykefraværsstatistikkDvhImport)
        dvhImportApplikasjon shouldContainLog "Starter import av sykefraværsstatistikk for alle statistikkkategorier".toRegex()
        dvhImportApplikasjon shouldContainLog "Jobb 'alleKategorierSykefraværsstatistikkDvhImport' ferdig".toRegex()
    }

    @Test
    fun `skal ignorere jobber applikasjon ikke kjenner`() {
        val jobbSomIkkeSkalKjøreIDvhImportApplikasjon = iaSakEksport
        kafkaContainer.sendJobbMelding(iaSakEksport)
        dvhImportApplikasjon shouldContainLog "Jobb '$jobbSomIkkeSkalKjøreIDvhImportApplikasjon' ignorert".toRegex()
    }
}