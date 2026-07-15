# Del 2 — Orkestrering, validering og alerts

Del 2 er hovedarbeidet som gjenstår før vi går live **3. september 2026**. På overordnet
nivå legger vi til tre ting:

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

#### `automatisering_import_orkestrering_jobber` — de 7 stegene per kjøring

| Kolonne | Type | Beskrivelse |
|---|---|---|
| `id` | serial PK | |
| `navn` | varchar | IMPORT_LAND, IMPORT_SEKTOR, IMPORT_NARING, IMPORT_NARINGSKODE, IMPORT_BRANSJE, IMPORT_VIRKSOMHET, IMPORT_VIRKSOMHET_METADATA |
| `publiseringsdato_id` | int FK → `publiseringsdato(id)` | |
| `rekkefolge` | smallint | 1..7 |
| `status` | varchar null | status for steget |
| `kontroll` | varchar null | OK eller årsak til feil (se under) |
| `start_dato` / `slutt_dato` | timestamp null | |
| `antall_rader_lest` | int | antall rader lest i JSON-filen |
| `antall_sendt_paa_kafka` | int | antall meldinger sendt på topic |
| `sf_prosent` | numeric null | kalkulert sykefraværsprosent |

`UNIQUE(publiseringsdato_id, navn)`.

### Orkestreringslogikk (på publiseringsdato)

Er det publiseringsdato i dag? Hvis ja, slå opp lock-rad for `publiseringsdato_id`:

1. **Finnes + `status = STARTET`/`FERDIG`** → ikke kjør (pågår eller ferdig). Dette hindrer
   dobbel/parallell kjøring på flere pods.
2. **Finnes + `status = FEILET`** → sett `STARTET`, gjenoppta fra siste FEILET-steg i
   `automatisering_import_orkestrering_jobber`.
3. **Finnes ikke** → skriv ny rad (`STARTET`), start importen fra første steg etter
   `rekkefolge`.
   - Hvis import feiler → `status = FEILET`.
   - Hvis import er OK → `status = FERDIG` (og marker `publiseringsdato.prosessert`).

### Auto-parameter (resume uten manuell input)

Master-jobben skal automatisk vite hvor den skal starte, basert på tabellene — slik at man
slipper å spesifisere `2025-4:BRANSJE` manuelt:

- Har jobben kjørt allerede? Finnes det 7 rader koblet til gjeldende `publiseringsdato_id`?
- Hvis ingen steg er funnet → opprett de 7 stegene.
- Hva er status på stegene? (NULL / OK / FEILET)
- Hva er neste steg? (siste steg med status FEILET, eller første ikke-kjørte)
- Fortsett importen på siste FEILET-steg, eller start på det første.

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
  og filtrerer bort støyen.
- Det kan hende vi oppdager **mer støy** enn vi trodde vi hadde. Dette kan vi logge.

#### `sf_prosent` dobbeltsjekkes før sending
`sf_prosent` blir dobbeltsjekket før vi sender dataen videre til Salesforce og FAGER.

For LAND, SEKTOR, NÆRING, NÆRINGSKODE og VIRKSOMHET (ikke bransje/metadata):

1. Les filen.
2. Kalkuler `SF-prosent-kalkulert-av-PIA = Round(2){ SUM(tapteDV) / SUM(muligeDV) * 100 }`.
   - For LAND: sammenlign mot `land.prosent` i filen.
   - For øvrige kategorier: aggregert prosent skal være lik `land.prosent` (hver kategori er
     en full partisjon av hele landet).
3. Hvis **ulik** → STOP import, `logger.error`, sett `status = FEILET` og
   `kontroll = SF_PROSENT_FEIL`.
4. Hvis **lik** → send Kafka, lagre `prosent` i `sf_prosent`, `kontroll = OK`,
   `status = FERDIG`.

Vi kan gjenbruke `kalkulerOgLoggSykefraværsprosent()` for kalkuleringen.

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
| Bransje (leser næring på nytt) | 50–150 |
| Virksomhet | 300 000–500 000 |
| Virksomhet_metadata | 300 000–500 000 |

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

## Oppsummert estimat for del 2

| Oppgave | Estimat |
|---|---|
| Orkestrering (2 koding + 2 testing) | 4 dager |
| Verifisering/validering av data-input | 3 dager |
| Alerts på Slack | 1 dag |
| Test i dev | 2 dager |
| **Totalt** | **10 dager** |

> Forutsetter at åpne spørsmål er avklart (se `/memories`-plan): steg-status (beholde
> STARTET?), eksakt likhet vs. toleranse for `sf_prosent`, og hva "start nytt kvartal dersom
> forrige ikke er OK" presist betyr.
