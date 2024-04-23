package no.nav.pia.dvhimport.konfigurasjon.plugins

import io.ktor.server.application.*
import io.ktor.server.routing.*
import no.nav.pia.dvhimport.http.helse

fun Application.configureRouting() {
    routing {
        helse()
    }
}
