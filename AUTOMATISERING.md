# Automatisering av DVH-import av sykefraværsstatistikk

> **📚 Ny dokumentasjonsstruktur:** Det videre automatiseringsarbeidet er nå delt opp i
> [docs/automatisering/](docs/automatisering/README.md):
> - [README](docs/automatisering/README.md) — overordnet intro og leseguide
> - [Del 1 — Grunnautomatisering](docs/automatisering/del-1-grunnautomatisering.md) (i hovedsak ferdig — denne filen er endringsloggen)
> - [Del 2 — Orkestrering, validering og alerts](docs/automatisering/del-2-orkestrering-validering-alerts.md) (hovedarbeidet som gjenstår)
> - [Del 3 — Dynamisk tilnærming og downstream-automatisering](docs/automatisering/del-3-dynamisk-tilnaerming.md) (lavere prioritet)
>
> Denne filen beholdes som detaljert endringslogg for del 1.

## Bakgrunn

Kvartalsvis import av sykefraværsstatistikk fra DVH (Datavarehus) krevde manuell kjøring av 7 separate GitHub Actions-jobber i riktig rekkefølge. Det tok ~3.5 timer, feil ble slukt stille, og det var ingen måte å vite om noe gikk galt underveis.

### Tidligere manuell prosess for publiseringsdatoer

Publiseringsdatoer ble håndtert helt manuelt:
1. I ca. november spurte vi DVH på Slack om publiseringsdatoene for neste år var publisert
2. Når DVH bekreftet, kjørte vi `publiseringsdatoDvhImport` manuelt med riktig `årstallOgKvartal`-parameter
3. Publiseringsdatoene ble da sendt til Kafka og konsumert av lydia-api

### Mål

Automatisere hele prosessen slik at importen kjører selv på publiseringsdagen, med god feilhåndtering og varsling.

**Viktigste krav:** Importen (`importAlleStatistikkKategorier`) må **stoppe ved feil og ikke starte på nytt automatisk**. Manuell restart med `startFra`-parameter er eneste måte å gjenoppta etter feil.

---

## Arkitektur

### To daglige CronJobs

| Jobb | Tidspunkt | Ansvar |
|---|---|---|
| `publiseringsdatoDvhImport` | Daglig kl 21:00 | Les GCS → lagre publiseringsdato til DB → send endringer til Kafka |
| `sjekkPubliseringsdatoOgImporter` | Daglig kl 08:05 | Les DB → er det publiseringsdato i dag? → kjør import |

- `publiseringsdatoDvhImport` kjører først — holder DB oppdatert med publiseringsdatoer fra DVH
- `sjekkPubliseringsdatoOgImporter` sjekker DB for rader med `dato = i dag` og `prosessert = false`
  - Ja → kjør `importAlleStatistikkKategorier` (LAND → SEKTOR → NÆRING → NÆRINGSKODE → BRANSJE → VIRKSOMHET → VIRKSOMHET_METADATA), marker `prosessert = true` ved suksess
  - Nei → logg "Ikke publiseringsdato i dag", avslutt
- Feil midtveis: raden forblir `prosessert = false`. Restartes manuelt med `startFra`-parameter (f.eks. `2025-4:BRANSJE`)
- Jobben kjøres bare én gang per pod (NaisJob med `completions: 1`)

### Dry-run-modus

`EksportProdusent.sendMelding()` kan skrus av med environment-variabel, slik at hele importkjeden kjøres uten å sende Kafka-meldinger. Brukes for å verifisere at importen fungerer i prod uten å påvirke downstream-systemer.

---

## Konkret plan og tidslinje

### Fase 1 — Gjøre ferdig kode (nå → 25. mai)

- [x] **1a)** `publiseringsdatoDvhImport` → DB-skriving
  - `ImportService` får `PubliseringsdatoRepository?` som parameter
  - `importPubliseringsdatoOgSendTilKafka` kaller `repository.lagrePubliseringsdato()` for hver publiseringsdato
  - `lagrePubliseringsdato` resetter `prosessert = false` når datoen endres
  - Tester
- [x] **1b)** `publiseringsdatoDvhImport` sender Kafka kun ved endring
  - `lagrePubliseringsdato` returnerer `LagreResultat` (NY/OPPDATERT/UENDRET) via PostgreSQL `xmax`-kolonnen
  - Kafka-melding sendes kun ved NY eller OPPDATERT
- [x] **1c)** `publiseringsdatoDvhImport` parameter-uavhengig
  - `importPubliseringsdatoer()` beregner selv årstall: inneværende + neste år
  - Fil som ikke finnes ennå hoppes over med loggmelding
  - `importMetadata` renamed til `importVirksomhetMetadata` (PUBLISERINGSDATO-grenen fjernet)
- [x] **1d)** Dry-run-mekanisme
  - Environment-variabel (`DRY_RUN=true`) som skrur av `EksportProdusent.sendMelding()`
  - Logger `DRY RUN: Ville sendt melding til topic X` i stedet
  - `NaisEnvironment` → `ImportService` → `EksportProdusent` (dryRun-flagg gjennom hele kjeden)
  - Begge miljøer starter med `dryRun: "true"` — endres til `"false"` i Fase 5
- [x] **1e)** `publiseringsdatoDvhImport` som daglig CronJob
  - `nais_daglig.yaml` parametrisert med `{{ SCHEDULE }}` (var hardkodet `0 6 * * *`)
  - Ny deploy-jobb i pia-jobbsender `build.yaml` med `SCHEDULE=0 19 * * *` (19:00 UTC = 21:00 CEST)
  - Deployes til dev og prod
- [ ] **1f)** Deploy til dev og verifiser infrastruktur
  - Flyway migrerer DB, CronJobs deployes, manuell kjøring fungerer

### Fase 2 — Verifisering i dev (~25. mai)

- [ ] **2a)** Dry-run av `sjekkPubliseringsdatoOgImporter` i dev
  - Sett inn testrad i `publiseringsdato`-tabellen med dagens dato
  - Kjør manuelt → verifiser at `importAlleStatistikkKategorier` kjører korrekt
  - Verifiser at Kafka-meldinger IKKE sendes (dry-run)
- [ ] **2b)** La CronJobs kjøre automatisk i dev
  - Overvåk via Grafana/logger at alt ser riktig ut
- [ ] **2c)** Grafana-dashboard
  - Siste kjøring, antall rader per kategori, neste publiseringsdato
- [ ] **2d)** Verifiser at eksterne konsumenter tåler duplikate meldinger
  - Sjekk om team Fager og team Salesforce bruker upsert eller tilsvarende idempotent håndtering
  - Hvis ikke: duplikater ved restart med `startFra` kan gi feil data hos dem (f.eks. dobbeltelling hvis de bruker INSERT uten ON CONFLICT, eller append i stedet for overskriving)
  - Evt. løsninger:
    1. **Avtale med teamene** at de må tåle duplikater (enklest — krever ingen kodeendring hos oss)
    2. **Offset-tracking per bit** — lagre siste vellykkede bit-indeks i DB. Eksempel: VIRKSOMHET har ~200k rader som sendes i biter à 1000. Hvis det feiler på bit 47 (rad 47000), lagrer vi `startFraBit = 47`. Ved restart hopper vi over de første 46 bitene og sender fra bit 47. Maks 1000 duplikater i stedet for 47000. Krever ny kolonne i `publiseringsdato`-tabellen eller egen tracking-tabell.

### Fase 3 — Dry-run i prod (28. mai)

28. mai er publiseringsdag for sykefraværsstatistikk. Vi bruker den "gamle metoden" for selve importen, men kjører dry-run parallelt.

- [ ] **3a)** Deploy med `DRY_RUN=true` i prod
- [ ] **3b)** `sjekkPubliseringsdatoOgImporter` kjører automatisk kl 08:05
  - Verifiser i logger at hele importkjeden kjøres korrekt
  - Verifiser at ingen Kafka-meldinger sendes
- [ ] **3c)** Sammenlign oppsummeringslogg (antall rader per kategori) med manuell import
  - Bør matche 1:1

### Fase 4 — Refaktorering (august 2026)

- [ ] **4a)** Opprett `PubliseringsdatoService` i `publiseringsdato/`-pakken
  - Flytt `sjekkPubliseringsdatoOgStartImport`, `importPubliseringsdatoOgSendTilKafka` og `nestePubliseringsdato` fra `ImportService`
  - Flytt `Publiseringsdato`-companion-funksjoner (`antallDagerTilPubliseringsdato`, `erFørPubliseringsdato`, `sjekkPubliseringErIDag`, `timeZone`) til metoder på `PubliseringsdatoService`
- [ ] **4b)** Splitt `ImportService` — statistikkimport i egen `StatistikkImportService`, `ImportService` kun som orkestrator
- [ ] **4c)** Rydd i `EksportProdusent` — vurder egen Kafka-produsent for publiseringsdato i stedet for felles `EksportMelding<T>`-hierarki med `when`-sjekker
- [ ] **4d)** Migrer fra `kotlinx.datetime` til `kotlin.time` — `Clock`, `Instant`, `LocalDateTime` og `TimeZone` er deprecated. Berører `ImportService`, `Publiseringsdato`, `PubliseringsdatoFraDvhDto` (serializers) og tester. Krever gjennomgang av `DvhDatoMedTidSerializer`

### Fase 5 — Robustgjøring av nedstrøms Kafka-kjede (august 2026)

Automatiseringen av dvh-import hjelper lite hvis meldinger forsvinner stille i neste ledd. Disse fiksene sikrer at hele kjeden GCS → dvh-import → pia-sykefravarsstatistikk → lydia-api er pålitelig.

#### Fire-and-forget i KafkaProdusent (pia-sykefravarsstatistikk + lydia-api)

`KafkaProdusent.sendMelding()` i begge appene kaller `producer.send()` uten callback — Kafka-feil (nede, auth, topic finnes ikke) forsvinner stille. Meldinger kan mistes uten at noen merker det.

Fiksen er identisk med det vi allerede har i `EksportProdusent` i dvh-import: `AtomicReference<Exception>` + callback + `flush()`.

- [ ] **5a)** PR til `pia-sykefravarsstatistikk`: Legg til callback og flush i `KafkaProdusent`
- [ ] **5b)** PR til `lydia-api`: Samme fix i `ia/eksport/KafkaProdusent.kt`

#### Per-melding feilhåndtering i pia-sykefravarsstatistikk consumers

Alle consumers (`SykefraværsstatistikkConsumer`, `VirksomhetMetadataConsumer`, `PubliseringsdatoConsumer`) mangler try/catch rundt deserialisering per record. Én korrupt melding → exception → ingen meldinger committes → uendelig retry-loop → pod restart-loop.

Relevant fordi meldingene som konsumeres er de vi sender fra dvh-import. Én edge case i DVH-dataene kan stoppe hele nedstrøms-kjeden uten varsling.

- [ ] **5c)** PR til `pia-sykefravarsstatistikk`: Wrap deserialisering i try/catch per record, logg og hopp over ugyldige meldinger

### Fase 6 — Automatisk import i prod (2. september)

- [ ] **6a)** Skru av dry-run (`DRY_RUN=false`)
- [ ] **6b)** `sjekkPubliseringsdatoOgImporter` kjører automatisk — ingen manuell jobb trengs
- [ ] **6c)** Overvåk via Grafana-dashboard og alerts

### Fase 7 — Validering av importdata før Kafka-sending (~oktober 2026)

Før vi sender Kafka-meldinger bør vi validere at GCS-filene ser fornuftige ut. Eksempler:
- **Antall rader innenfor forventet område** — f.eks. `land.json` skal ha ~1 rad, `sektor.json` ~4, `næring.json` ~80-100, `virksomhet.json` ~200k. Hvis en fil er tom eller har 10x forventet størrelse, er det sannsynligvis noe galt med filen fra DVH.
- **Format-validering** — sjekk at obligatoriske felter finnes og har riktige typer.
- **Sammenlign med forrige import** — store avvik i antall rader kan tyde på feil.

Fordelen er at vi oppdager feil *før* vi sender data videre til `pia-sykefravarsstatistikk`, i stedet for å oppdage det etterpå.

- [ ] **7a)** Definer forventede rader per kategori
- [ ] **7b)** Implementer validering i `importForStatistikkKategori` — feil før `sendTilKafka()`
- [ ] **7c)** Tester for valideringslogikken

### Fase 8 — Verifiser publiseringsdato-jobb (oktober/november 2026)

DVH legger typisk nye `publiseringsdato.json` i GCS i oktober/november. Første gang vi ser om `publiseringsdatoDvhImport`-jobben faktisk plukker opp nye datoer og skriver til DB.

- [ ] **8a)** Sjekk at nye publiseringsdatoer for 2027 dukker opp i DB
- [ ] **8b)** Sjekk at Kafka-melding sendes (kun ved endring)

### Fase 9 — Full E2E-verifisering (februar 2027)

Første import for 2027. Første gang begge jobbene kjører sammen i produksjon med ekte data.

- [ ] **9a)** `publiseringsdatoDvhImport` har allerede lagret dato for Q4-2026 → `sjekkPubliseringsdatoOgImporter` finner den → import kjøres automatisk
- [ ] **9b)** Hele kjeden fungerer uten manuell inngripen

---

## Status — hva er gjort

### ✅ Gjort

#### Feilhåndtering (ImportService.kt)
- Alle catch-blokker som slukte feil (`return emptyList()`) er endret til `logger.error + throw`
- `bucket ikke funnet` → `throw IllegalStateException` (var: `logger.error + return`)
- `hentInnhold()` kaster feil ved tom/null data (var: `return emptyList()`)
- Alle exceptions bobler nå opp og stopper importen

#### Kafka-sending med feilhåndtering (EksportProdusent.kt)
- `producer.send()` var fire-and-forget — Kafka-feil (f.eks. `TopicAuthorizationException`) ble aldri fanget
- `sendMelding()` har nå callback som fanger feil i `AtomicReference`
- Ny metode `flushOgSjekkFeil()` — kaller `producer.flush()`, sjekker feil, kaster `KafkaSendException`
- Kalles etter sending i `sendTilKafka()`, `sendMetadataTilKafka()` og `importPubliseringsdatoOgSendTilKafka()`
- For VIRKSOMHET og VIRKSOMHET_METADATA (som sender i biter à 1000): flush per bit — feil oppdages etter maks 1000 meldinger

**Hvorfor `AtomicReference<Exception?>` + flush?**
`producer.send()` er asynkron — meldingen legges i en intern buffer og sendes i bakgrunnen. Feil oppstår først når callback-en trigges, lenge etter at `sendMelding()` returnerer. `førsteFeil.compareAndSet(null, exception)` lagrer den første feilen thread-safe (flere callbacks kan kjøre parallelt). `producer.flush()` blokkerer til alle ventende meldinger er bekreftet/feilet — altså til alle callbacks har kjørt. Deretter sjekker vi `førsteFeil`: er den satt, kaster vi exception og stopper importen.

#### "Start fra"-parameter
- `importAlleStatistikkKategorier(årstallOgKvartal, startFra)` med `dropWhile` for å hoppe over allerede importerte kategorier
- Nytt parameterformat: `2025-4:NÆRINGSKODE` (valgfri startFra etter kolon)
- Reduserer duplikater til Kafka ved feil: hopper over allerede fullførte kategorier
- **Forutsetning:** Konsumentene er idempotente — vi antar at alle som leser fra Kafka-topicene bruker upsert eller tilsvarende, slik at duplikater er ufarlige. Vi kontrollerer egne konsumenter (lydia-api, pia-sykefravarsstatistikk), men andre teams (Fager, Salesforce) konsumerer også. Ved restart fra f.eks. `VIRKSOMHET` vil noen meldinger sendes på nytt (maks 1000, fordi flush skjer per bit). Vi unngår å kjøre allerede fullførte kategorier (LAND, SEKTOR, ...) på nytt. Se punkt 2d.

#### VIRKSOMHET_METADATA kjøres automatisk
- `importAlleStatistikkKategorier` kjører alltid VIRKSOMHET_METADATA til slutt, uavhengig av `startFra`
- Før måtte denne kjøres som en separat manuell jobb

#### Per-melding feilhåndtering (Jobblytter.kt)
- try/catch rundt hele `when`-blokken per Kafka-record
- Unntak i én melding krasjer ikke lenger hele consumer-loopen

#### Oppsummeringslogg
- Alle import-funksjoner returnerer antall rader
- Etter fullført import logges: `Import ferdig for 2025-Q1: LAND=1, SEKTOR=4, NÆRING=88, ...`
- Gjør det enkelt å spotte avvik i Grafana

#### Database-infrastruktur
- Cloud SQL POSTGRES_17 provisjonert i nais.yaml (db-f1-micro)
- Flyway-migrasjon (`V1__create_publiseringsdato.sql`)
- DataSourceBuilder (HikariCP + Flyway)
- HikariCP med `maximumPoolSize = 3`. De andre appene bruker 10, men siden dette er en NaisJob som aldri trenger mer enn én tilkobling om gangen, er 3 mer enn nok
- PubliseringsdatoRepository med `hentUprosesserteForDato`, `markerSomProsessert`, `lagrePubliseringsdato`
- Nullable — appen fungerer fortsatt uten database

#### DB-skriving i `publiseringsdatoDvhImport` + Kafka kun ved endring
- `ImportService` har `PubliseringsdatoRepository?` som valgfri parameter
- `importPubliseringsdatoOgSendTilKafka` lagrer hver publiseringsdato til DB via `lagrePubliseringsdato`
- `lagrePubliseringsdato` returnerer `LagreResultat` (NY/OPPDATERT/UENDRET) via PostgreSQL `xmax`-kolonnen — `xmax` er en skjult systemkolonne som er 0 for nye rader og != 0 for oppdaterte (del av MVCC)
- Kafka-melding sendes kun ved NY eller OPPDATERT
- Ved datoendring resettes `prosessert = false` automatisk — slik at `sjekkPubliseringsdatoOgImporter` plukker opp den nye datoen
- Uten database (repository = null) sendes Kafka-meldinger for alle publiseringsdatoer (bakoverkompatibelt)

#### ia-felles v3.0.1
- Ny `sjekkPubliseringsdatoOgImporter` enum i `Jobb`
- Pushet og tagget, tilgjengelig via JitPack

#### CronJob: `sjekkPubliseringsdatoOgImporter` (pia-jobbsender)
- NaisJob med `schedule: "0 6 * * *"` (06:00 UTC = 08:00 CEST)
- Lagt til i manuell-deploy dropdown
- Deploy-jobb i build.yaml for dev + prod

#### Alerts
- `alerts.yaml` med PrometheusRule som trigger på alle ERROR-logger fra pia-dvh-import
- `deploy-alerts.yaml` GitHub Actions workflow for deploy til dev + prod
- Severity `warning` → Slack, ikke PagerDuty. Greit fordi 08:05-jobben (den viktige) feiler i arbeidstiden. 21:00-jobben har lav konsekvens ved feil — oppdager det neste morgen via Slack.

#### Pakkestruktur `publiseringsdato/`
- Ny pakke `importjobb/publiseringsdato/` samler all publiseringsdato-relatert kode
- `PubliseringsdatoRepository` + `PubliseringsdatoDto` + `LagreResultat` (DB-typer)
- `PubliseringsdatoFraDvhDto` + `PubliseringsdatoKafkaDto` + `Publiseringsdato` + `NestePubliseringsdato` (domene/Kafka)
- Kafka-DTO renamed `PubliseringsdatoDto` → `PubliseringsdatoKafkaDto` for å unngå navnekonflikt med DB-DTO
- `sjekkPubliseringsdatoOgStartImport` flyttet fra `Jobblytter` til `ImportService`

#### `publiseringsdatoDvhImport` parameter-uavhengig
- `importPubliseringsdatoer()` bruker `LocalDate.now().year` til å beregne hvilke år som skal leses: inneværende + neste år. F.eks. i 2026 leser den GCS-filer for både 2026 og 2027. Hvis filen for 2027 ikke finnes ennå, logges det og hoppes over. Når DVH legger ut `publiseringsdato_2027.json` i oktober/november, plukker neste kjøring den opp automatisk.
- `importPubliseringsdatoForÅr(årstall)` erstatter `importPubliseringsdato(årstallOgKvartal)` — tar kun årstall, kvartalet var ubrukt
- Deduplisering med `distinctBy { rapportPeriode }` for å unngå duplikater hvis begge år har samme data
- `Jobblytter` kaller `importService.importPubliseringsdatoer()` uten parameter
- Manuell kjøring via `publiseringsdatoDvhImport` i pia-jobbsender fungerer fortsatt (uten parameter)

#### Dry-run-mekanisme (EksportProdusent.kt)
- `DRY_RUN` env-variabel leses i `NaisEnvironment`, sendes via `ImportService` til `EksportProdusent`
- `sendMelding()` sjekker `dryRun`-flagget — logger topic og nøkkel, returnerer uten å sende
- Topic-logikk refaktorert til `tilTopic()` for å unngå duplisering
- Oppstartslogg i `Application.kt` når dry-run er aktiv
- GCS-lesing, parsing, DB-lagring og oppsummeringslogg kjører normalt — bare Kafka-sending hoppes over

#### CronJob: `publiseringsdatoDvhImport` (pia-jobbsender)
- `nais_daglig.yaml` parametrisert med `{{ SCHEDULE }}` — brukes av både `sjekkPubliseringsdatoOgImporter` (06:00 UTC) og `publiseringsdatoDvhImport` (19:00 UTC)
- Ny deploy-jobb `deploy-publiseringsdato-dvh-import` i `build.yaml` for dev + prod
- Allerede i manuell-deploy dropdown (fra før)

#### Tester
- 6 repository-tester (lagrePubliseringsdato, update med prosessert-reset, samme dato returnerer false, markerSomProsessert, ingen match, allerede prosessert)
- 5 enhetstester for `sjekkPubliseringsdatoOgStartImport` (mockk)
- 4 enhetstester for `EksportProdusent` (MockProducer: vellykket sending, TopicAuthorizationException, flere feil, feil-reset)
- Oppdaterte integrasjonstester for ia-felles v3.0.1 (SYKEHJEM-næringskoder)
- Oppdaterte feilmeldinger i ImportServiceIntegrasjonstest

### 🔲 Gjenstår (Fase 1)

| Oppgave | Referanse | Status |
|---|---|---|
| DB-skriving i `publiseringsdatoDvhImport` | 1a | ✅ |
| Kafka kun ved endring (LagreResultat) | 1b | ✅ |
| `publiseringsdatoDvhImport` parameter-uavhengig | 1c | ✅ |
| Dry-run-mekanisme | 1d | ✅ |
| CronJob for `publiseringsdatoDvhImport` | 1e | ✅ |
| Deploy + verifiser i dev | 1f | 🔲 |

---

## Berørte repoer

| Repo | Endringer | Status |
|---|---|---|
| `ia-felles` | Ny `sjekkPubliseringsdatoOgImporter` enum (v3.0.1) | ✅ Pushet |
| `pia-dvh-import` | Feilhåndtering, startFra, DB, alerts, dry-run | ⬜ Ikke pushet |
| `pia-jobbsender` | CronJobs, manuell-deploy | ⬜ Ikke pushet |

## Rekkefølge for deploy

1. `ia-felles` v3.0.1 (allerede pushet + tagget)
2. `pia-dvh-import` (alle kodeendringer)
3. `pia-jobbsender` (CronJobs som trigger de nye jobbene)
