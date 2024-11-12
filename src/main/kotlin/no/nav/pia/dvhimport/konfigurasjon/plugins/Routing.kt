package no.nav.pia.dvhimport.konfigurasjon.plugins

import io.ktor.server.application.Application
import io.ktor.server.routing.routing
import no.nav.pia.dvhimport.http.helse

fun Application.configureRouting() {
    routing {
        helse()
    }
}
