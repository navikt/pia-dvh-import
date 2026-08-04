package no.nav.pia.dvhimport.importjobb.kafka

import ia.felles.integrasjoner.jobbsender.Jobb.iaSakEksport
import no.nav.pia.dvhimport.helper.TestContainerHelper
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.dvhImportApplikasjon
import no.nav.pia.dvhimport.helper.TestContainerHelper.Companion.shouldContainLog
import kotlin.test.Test

class JobblytterEdgeCasesTest {
    private val kafkaContainer = TestContainerHelper.kafka

    @Test
    fun `skal ikke feile dersom meldinger er om en ukjent jobb til en annen applikasjon`() {
        kafkaContainer.sendJobbMelding(jobbnavn = "denne-skal-få-deserializer-til-å-krasje-ved-Jobb-Enum", applikasjon = "lydia-api")
        dvhImportApplikasjon shouldContainLog "Mottok en Kafka melding hvor målapplikasjonen er: 'lydia-api'".toRegex()
    }

    @Test
    fun `skal ikke prosessere meldinger som ikke er til pia-dvh-import`() {
        kafkaContainer.sendJobbMelding(jobbnavn = iaSakEksport.name, applikasjon = "lydia-api")
        dvhImportApplikasjon shouldContainLog "Mottok en Kafka melding hvor målapplikasjonen er: 'lydia-api'".toRegex()
    }
}
