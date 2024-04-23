package no.nav.pia.dvhimport.http

import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Route.helse() {
    get("internal/isalive") {
        call.respond("OK")
    }
    get("internal/isready") {
        call.respond("OK")
    }
}
