# Del 1 — Grunnautomatisering

Del 1 etablerer fundamentet for automatisk import: daglige CronJobs, robust feilhåndtering,
dry-run-modus, `startFra`-parameter, database for publiseringsdatoer og alerts.

> **Detaljert endringslogg:** Den fullstendige statusen og endringsloggen for del 1 ligger i
> [../../AUTOMATISERING.md](../../AUTOMATISERING.md). Denne filen oppsummerer arkitekturen og
> hovedgrepene.

## Arkitektur: to daglige CronJobs

| Jobb | Tidspunkt | Ansvar |
|---|---|---|
| `publiseringsdatoDvhImport` | Daglig kl 21:00 | Les GCS → lagre publiseringsdato til DB → send endringer til Kafka |
| `sjekkPubliseringsdatoOgImporter` | Daglig kl 08:05 | Les DB → er det publiseringsdato i dag? → kjør import |

- `publiseringsdatoDvhImport` kjører først — holder DB oppdatert med publiseringsdatoer fra DVH.
- `sjekkPubliseringsdatoOgImporter` sjekker DB for rader med `dato = i dag` og `prosessert = false`:
  - **Ja** → kjør `importAlleStatistikkKategorier` (LAND → SEKTOR → NÆRING → NÆRINGSKODE →
    BRANSJE → VIRKSOMHET → VIRKSOMHET_METADATA), marker `prosessert = true` ved suksess.
  - **Nei** → logg "Ikke publiseringsdato i dag", avslutt.
- Feil midtveis: raden forblir `prosessert = false`. Restartes manuelt med `startFra`-parameter
  (f.eks. `2025-4:BRANSJE`).
- Jobben kjøres bare én gang per pod (NaisJob med `completions: 1`).

## Hovedgrep i del 1

### Feilhåndtering
- Alle catch-blokker som slukte feil (`return emptyList()`) er endret til `logger.error + throw`.
- Alle exceptions bobler nå opp og **stopper importen** i stedet for å fortsette stille.

### Kafka-sending med feilhåndtering
- `EksportProdusent.sendMelding()` har callback som fanger feil i `AtomicReference`.
- `flushOgSjekkFeil()` blokkerer til alle meldinger er bekreftet/feilet, og kaster
  `KafkaSendException` ved feil.
- For VIRKSOMHET / VIRKSOMHET_METADATA (sendes i biter à 1000): flush per bit.

### `startFra`-parameter
- `importAlleStatistikkKategorier(årstallOgKvartal, startFra)` hopper over allerede
  importerte kategorier med `dropWhile`.
- Parameterformat: `2025-4:NÆRINGSKODE`.
- **Forutsetning:** konsumentene er idempotente (upsert). Ved restart fra f.eks. VIRKSOMHET
  sendes maks 1000 duplikater på nytt.

### Dry-run-modus
- `DRY_RUN`-env-variabel skrur av `EksportProdusent.sendMelding()`.
- Hele importkjeden (GCS-lesing, parsing, DB-lagring, oppsummeringslogg) kjøres uten å sende
  Kafka-meldinger. Brukes for å verifisere import i prod uten å påvirke downstream.

### Database
- Cloud SQL POSTGRES_17, HikariCP (`maximumPoolSize = 3`), Flyway-migrasjoner.
- `PubliseringsdatoRepository` med `hentUprosessertForDato`, `markerSomProsessert`,
  `lagrePubliseringsdato` (returnerer `LagreResultat`: NY/OPPDATERT/UENDRET).
- Kafka-melding for publiseringsdato sendes kun ved NY eller OPPDATERT.

### Alerts
- `alerts.yaml` (PrometheusRule) trigger på alle ERROR-logger fra `pia-dvh-import`.
- Severity `warning` → Slack (ikke PagerDuty), siden den viktige 08:05-jobben feiler i
  arbeidstiden.

## Status

Del 1 er i hovedsak ferdig. Det som gjenstår er deploy + verifisering i dev, samt aktivering
av selve den automatiske importen (se [../../TODO.md](../../TODO.md) — early return i
`sjekkPubliseringsdatoOgStartImport` må fjernes når vi er trygge på kjeden).

## Kjent blokkering: publiseringsdato-filen

`publiseringsdatoer.json` i GCS **slettes i desember** hvert år og publiseres først igjen
rundt **midten av oktober** året etter. `publiseringsdatoDvhImport` vil derfor feile med
"finner ikke fil" frem til DVH publiserer nye datoer (~oktober 2026). Se
[../møte-19-mai.md](../møte-19-mai.md) for detaljer.
