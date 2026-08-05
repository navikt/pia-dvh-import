# Del 2 — Orkestrering, validering og alerts

Del 2 var hovedarbeidet før go-live **3. september 2026**, og er nå **ferdig**. På overordnet
nivå la vi til tre ting:

1. **Orkestrering** — en selvkjørende mekanisme som starter automatisk på riktig tidspunkt,
   og som sørger for at *resume on error* skjer på riktig sted (unngår å sende samme data
   flere ganger).
2. **Validering** — kvalitetssikring av dataen Team Sykefravær sender oss, slik at vi er
   sikre på at dataen er riktig **før** vi sender den videre. Stopper hvis noe er feil.
3. **Oversikt** — alerts på Slack i en åpen kanal slik at alle interessenter (f.eks. FAGER
   og Salesforce) kan få nyttig informasjon.

**Estimat: ~10 dager med utvikling (Brage).**

---

## 1. Orkestrering

En selvkjørende orkestreringsmekanisme som startes automatisk på riktig tidspunkt, og som
sørger for at *resume on error* skjer på riktig sted.

### To nye DB-tabeller

#### `automatisering_import_lock` — orkestrering + lås (1 rad per import-kjøring)

| Kolonne | Type | Beskrivelse |
|---|---|---|
| `id` | serial PK | |
| `publiseringsdato_id` | int FK → `publiseringsdato(id)`, **UNIQUE** | én rad per kvartal-import |
| `status` | varchar | `STARTET` \| `FEILET` \| `FERDIG` |
| `start_dato` | timestamp | |
| `slutt_dato` | timestamp null | |

Denne raden er **låsepunktet** og gir total-status ("er alt bra?").

#### `automatisering_import_steg` — de 7 stegene per kjøring

| Kolonne | Type | Beskrivelse |
|---|---|---|
| `id` | serial PK | |
| `navn` | varchar | IMPORT_LAND, IMPORT_SEKTOR, IMPORT_NARING, IMPORT_NARINGSKODE, IMPORT_BRANSJE, IMPORT_VIRKSOMHET, IMPORT_VIRKSOMHET_METADATA |
| `publiseringsdato_id` | int FK → `publiseringsdato(id)` | |
| `rekkefolge` | smallint | 1..7 |
| `status` | varchar **NOT NULL** | `PLANLAGT` \| `STARTET` \| `VALIDERT` \| `FEILET` \| `FERDIG` |
| `kontroll` | varchar null | `OK` eller årsak til feil (se under) |
| `start_dato` / `slutt_dato` | timestamp null | |
| `antall_rader_lest` | int | antall rader lest i JSON-filen |
| `antall_sendt_paa_kafka` | int | antall meldinger sendt på topic |
| `sf_prosent` | numeric null | kalkulert sykefraværsprosent |

`UNIQUE(publiseringsdato_id, navn)`.

**Steg-livssyklus:** `PLANLAGT → STARTET → VALIDERT → FERDIG` (eller `FEILET`). Statusen er
`NOT NULL` — når de 7 stegene opprettes settes de eksplisitt til `PLANLAGT`. `FERDIG` brukes som fullført-status, konsistent med lås-tabellen.

### Orkestreringslogikk (på publiseringsdato)

Er det publiseringsdato i dag? Hvis ja, slå opp lock-rad for `publiseringsdato_id`:

1. **Finnes + `status = STARTET`/`FERDIG`** → ikke kjør (pågår eller ferdig). Dette hindrer
   dobbel/parallell kjøring på flere pods.
2. **Finnes + `status = FEILET`** → sett `STARTET`, gjenoppta fra siste FEILET-steg i
   `automatisering_import_steg`.
3. **Finnes ikke** → skriv ny rad (`STARTET`), start importen fra første steg etter
   `rekkefolge`.
   - Hvis import feiler → `status = FEILET`.
   - Hvis import er OK → `status = FERDIG` (og marker `publiseringsdato.prosessert`).

### Auto-parameter (resume uten manuell input)

Master-jobben skal automatisk vite hvor den skal starte, basert på tabellene — slik at man
slipper å spesifisere hvilket steg importen skal starte fra (man oppgir kun kvartalet
`2025-4`, ikke steget):

- Har jobben kjørt allerede? Finnes det 7 rader koblet til gjeldende `publiseringsdato_id`?
- Hvis ingen steg er funnet → opprett de 7 stegene (`PLANLAGT`).
- Hva er status på stegene? (`PLANLAGT` / `STARTET` / `VALIDERT` / `FEILET` / `FERDIG`)
- Hva er neste steg? (siste steg med status `FEILET`, eller første som ikke er ferdig i
  gjeldende fase — se to-fase over)
- Fortsett importen fra det steget.

**Forutsetninger:**
- `publiseringsdato`-tabellen har de datoene vi trenger (script eller automatisert).
- Vi starter import av et nytt kvartal kun dersom forrige kvartal er ferdig.
- Automatisk start skjer **kun på publiseringsdato**. Senere import krever manuell start
  med parameter `YYYY-K`.

### Estimat orkestrering: 4 dager (2 koding + 2 testing)

---

## 2. Validering og kvalitetssikring

I dag gjør vi noen **manuelle** sjekker. En manuell sjekk er alltid mindre sikker enn en
automatisk. Målet er å flytte sjekkene **før** data sendes, og automatisere dem.

### Hva vi sjekker manuelt i dag

- At vi har ca. riktig antall sektor, næring osv. som er importert.
- **Vi sjekker IKKE formatet** — dette skal vi nå gjøre.
- Vi sjekker at `sf_prosent` er riktig per kategori, **men i dag skjer det ETTER at dataen
  er sendt**, ikke før.

### Hva vi endrer

- Flytte `sf_prosent`-sjekken til **før** Kafka-sending → mindre etterarbeid, fordi vi er
  sikre på at dataen er riktig før den sendes.
- Vår egen utleding av **bransjer** blir også sikrere med denne ekstra verifiseringen.
- Vi skaffer oss **kontroll over dataen vår**.

### Nye sjekker vi ikke hadde fra før

#### Datakvalitet / støy
- Ca. **100 virksomheter med 7-sifret orgnr** har blitt importert hver eneste gang.
  Salesforce spurte oss nylig om dette. Orgnr skal ha 9 siffer — vi legger til en regex-sjekk
  og filtrerer bort støyen. Filteret gjelder i **både validerings- og sendefasen**, slik at
  støyen aldri når Kafka.
- Det kan hende vi oppdager **mer støy** enn vi trodde vi hadde. Dette kan vi logge.

#### `sf_prosent` dobbeltsjekkes før sending
`sf_prosent` blir dobbeltsjekket før vi sender dataen videre til Salesforce og FAGER.

For LAND, SEKTOR, NÆRING, NÆRINGSKODE og VIRKSOMHET (ikke bransje/metadata):

1. Les filen.
2. Kalkuler `SF-prosent-kalkulert-av-PIA = Round(2){ SUM(tapteDV) / SUM(muligeDV) * 100 }`.
   - For LAND: sammenlign mot `land.prosent` i filen.
   - For øvrige kategorier: aggregert prosent skal være lik `land.prosent` (hver kategori er
     en full partisjon av hele landet).
3. Hvis **ulik** → `logger.error`, sett `status = FEILET`, `kontroll = SF_PROSENT_FEIL`, og
   **abort hele importen** (ingen Kafka sendes for noe steg).
4. Hvis **lik** → sett `status = VALIDERT` og lagre `prosent` i `sf_prosent`. Kafka sendes
   **først i sendefasen**, når alle 7 steg er `VALIDERT` (se to-fase over).

Vi kan gjenbruke `kalkulerOgLoggSykefraværsprosent()` for kalkuleringen.

> I **dev** håndheves ikke `sf_prosent`-likheten: avviket logges (`ℹ️ Import ville ha feilet …`)
> og importen fortsetter, slik at testkjøringer med bevisst inkonsistente testdata går gjennom.
> **Lokal og prod** stopper fortsatt ved avvik. Cluster-styrt på `NAIS_CLUSTER_NAME`.

#### Strukturvalidering (regex) — `FEIL_STRUKTUR_I_INPUT_FIL`

Regex-validering av innholdet i JSON-filene:

| Fil | Felt | Regel | Regex |
|---|---|---|---|
| `land.json` | `land` | alltid "NO" | `^NO$` |
| `sektor.json` | `sektor` | ett siffer (1–4, margin) | `^\d$` |
| `naering.json` | `næring` | to siffer (eks. 01, 02) | `^\d{2}$` |
| `naeringskode.json` | `næringskode` | fem siffer (eks. 82123) | `^\d{5}$` |
| `virksomhet.json` | `orgnr` | 9 siffer | `^\d{9}$` |
| `virksomhet_metadata.json` | `orgnr` | 9 siffer | `^\d{9}$` |
| bransje | — | filen finnes ikke, utledes selv | ingen regex |

Vi bør også vurdere en sjekk på at årstall og kvartal er riktig (kan være dekket allerede).

#### Radgrenser — `FEIL_ANTALL_RADER_I_INPUT_FIL`

Etter lesing, før Kafka-sending. Utenfor intervallet → feil og stopp:

| Kategori | Forventet antall rader |
|---|---|
| Land | 1 |
| Sektor | 3–5 |
| Næring | 50–150 |
| Næringskode | 500–1500 |
| Bransje | ingen egen radgrense (utledes fra næring/næringskode) |
| Virksomhet | 300 000–500 000 |
| Virksomhet_metadata | 300 000–500 000 |

Tallene over gjelder **PROD**. Grensene velges per miljø på `NAIS_CLUSTER_NAME`: **DEV** bruker
1000–3000 for virksomhet/metadata, og **LOKAL** (test) bruker 0..MAX.

### `kontroll`-verdier

`NULL`, `OK`, `SF_PROSENT_FEIL`, `FEIL_ANTALL_RADER_I_INPUT_FIL`, `INPUT_FIL_IKKE_FUNNET`,
`FEIL_ÅRSTALL_ELLER_KVARTAL`, `FEIL_STRUKTUR_I_INPUT_FIL`, `KAFKA_ERROR`, `ANNET`.

### Estimat validering: 3 dager

---

## 3. Alerts på Slack

Alerts (f.eks. via metrics) i en åpen Slack-kanal, slik at også FAGER og Salesforce kan få
tilgang på informasjonen:

- "Det er publiseringsdato i dag — import har startet."
- "Import er ferdig." (eventuelt per kategori, men det er kanskje ikke nødvendig).
- Melding hvis et steg feiler (`status = FEILET`).

Stor fordel at FAGER og Salesforce kan få tilgang på loggene.

### Estimat alerts: 1 dag

---

## 4. Testing og verifisering

Prinsipp: **gode automatiske tester er viktigere enn testing i dev.** Med en solid
test-suite blir dev-verifiseringen triviell — den er egentlig bare determinisme-loopen under,
kjørt én gang mot ekte dev-bucket og dev-DB.

### Objekt-basert testdata

Testdataene bygges av `KonsistentTestdata` som **typede objekter** per kategori-fil, med et
gyldig, internt konsistent baseline-sett (sf_prosent summerer eksakt til `land.prosent`, så
validering passerer). En korrupsjon er da å overstyre ett enkelt felt:

| Korrupsjon | Forventet `kontroll` |
|---|---|
| Orgnr med 7 siffer | `FEIL_STRUKTUR_I_INPUT_FIL` |
| Næringskode med 10 siffer | `FEIL_STRUKTUR_I_INPUT_FIL` |
| For få / for mange rader i en kategori | `FEIL_ANTALL_RADER_I_INPUT_FIL` |
| sf_prosent som ikke summerer til `land.prosent` | `SF_PROSENT_FEIL` |
| Manglende fil | `INPUT_FIL_IKKE_FUNNET` |

### Negative tester (én per `kontroll`-verdi)

For hver korrupsjon: trigg `alleKategorierImport` og assert på DB:

- `automatisering_import_lock.status = FEILET`
- riktig `automatisering_import_steg.status = FEILET` med riktig `kontroll`
- **ingen Kafka sendt for noe steg** — to-fase-garantien: feil i valideringsfasen ⇒
  sendefasen kjøres aldri

### DRY_RUN som parameter

`DRY_RUN` sendes som et **per-melding-parameter**: `2026-1:DRY_RUN`. `Jobblytter.tilDryRun()`
parser tokenet og kjører **hele den orkestrerte stien** (lås, steg-rader, validering,
DB-oppdateringer og resume) — men hopper over **kun** Kafka-publiseringen. Dry-run er dermed en
realistisk generalprøve som tester orkestreringen ende-til-ende uten å sende data videre.
DB-sporene fra en dry-run ryddes **manuelt** etterpå. Dermed kan dev-verifisering trigges via
`pia-jobbsender` uten redeploy.

### Determinisme-/volumtest

Determinismen dekkes nå av en **automatisk test** (ikke dry-run). Testen kjører en **ekte**
import av ~1200 virksomheter tre ganger,
med sletting av lås- og steg-radene mellom hver kjøring, og sammenligner sluttilstanden per
steg (`status`, `antall_rader_lest`, `antall_sendt_paa_kafka`, `sf_prosent`). Alle tre
kjøringene skal gi identisk resultat. Sammenligningen skjer mot DB-snapshot (ikke
Kafka-drenering, som er flaky pga. delte consumer-offsets), noe som gir et robust og raskt
determinisme-bevis. Volumet legges på virksomhet/metadata (streaming-stien).

## Oppsummert estimat for del 2

| Oppgave | Estimat |
|---|---|
| Orkestrering (2 koding + 2 testing) | 4 dager |
| Verifisering/validering av data-input | 3 dager |
| Alerts på Slack | 1 dag |
| Test i dev | 2 dager |
| **Totalt** | **10 dager** |

