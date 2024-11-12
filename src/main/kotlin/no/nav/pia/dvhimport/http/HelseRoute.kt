package no.nav.pia.dvhimport.http

import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get

fun Route.helse() {
    get("internal/isalive") {
        call.respond("OK")
    }
    get("internal/isready") {
        call.respond("OK")
    }
}
