# Del 3 — Dynamisk tilnærming og downstream-automatisering

Del 3 samler oppgaver med **lavere prioritet**. Disse er ikke nødvendige for at den
automatiske importen i `pia-dvh-import` skal fungere, men de fjerner manuelle steg lenger
ned i kjeden (lydia-api, pia-sykefravarsstatistikk) og forbedrer observerbarhet.

DVH-kjeden i tre ledd:

```
pia-dvh-import  →  pia-sykefravarsstatistikk  →  lydia-api / Salesforce / FAGER
```

Del 1 og 2 automatiserer **ledd 1**. Del 3 handler om de manuelle stegene i ledd 2 og 3,
samt en mer prinsipiell diskusjon om statisk vs. dynamisk håndtering av årstall/kvartal.

---

## 1. Forbedre Grafana-logging

Bedre, mer strukturerte logger i Grafana slik at status på import (start, ferdig, feilet,
antall rader, sf_prosent) er lett å følge.

**Hva må gjøres:** Justere loggmeldinger / labels slik at de er lette å filtrere og bygge
dashboard på. Mye av dette faller naturlig sammen med alerts-arbeidet i del 2.

**Estimat: ~0,25 dag.**

---

## 2. Automatisk refresh av materialized view i lydia-api

I dag oppdaterer vi manuelt en materialized view i `lydia-api` hver gang nytt kvartal
publiseres, via en Flyway-migrasjon.

**Referansecommit:** `46673a3` i lydia-api.
- `V148__oppdatere_siste_publiseringsinfo_Q1_2026.sql`:
  ```sql
  insert into siste_publiseringsinfo(
      gjeldende_arstall, gjeldende_kvartal, siste_publiseringsdato, neste_publiseringsdato
  ) values (2026, 1, '2026-05-28', '2026-09-03');

  REFRESH MATERIALIZED VIEW virksomhetsstatistikk_for_prioritering;
  ```

**Hva må gjøres:** I stedet for en manuell migrasjon per kvartal, trigge `insert` i
`siste_publiseringsinfo` + `REFRESH MATERIALIZED VIEW virksomhetsstatistikk_for_prioritering`
automatisk når lydia-api har konsumert ferdig statistikk for et nytt kvartal (f.eks. ved en
Kafka-melding fra pia-dvh-import om at importen er ferdig, eller en intern scheduler).

**Estimat: ~1 dag.** Det meste er kjent (samme SQL), men det krever en ny trigger-mekanisme
i lydia-api og testing av at view-refresh skjer på riktig tidspunkt.

---

## 3. Automatisk tilgjengeliggjøring i pia-sykefravarsstatistikk

I dag bumper vi manuelt "nåværende kvartal" i `pia-sykefravarsstatistikk` hver gang nytt
kvartal publiseres.

**Referansecommit:** `8e11fa7` i pia-sykefravarsstatistikk.
- I `ImporttidspunktRepository.kt`:
  ```kotlin
  // fra
  val NÅVÆRENDE_KVARTAL = ÅrstallOgKvartal(årstall = 2025, kvartal = 4)
  // til
  val NÅVÆRENDE_KVARTAL = ÅrstallOgKvartal(årstall = 2026, kvartal = 1)
  ```

**Hva må gjøres:** Erstatte den hardkodede konstanten med en verdi som settes dynamisk —
enten fra publiseringsdato-dataen (som pia-dvh-import allerede eier) eller fra en melding om
fullført import.

**Estimat: ~0,5 dag** isolert sett (selve endringen er liten), men den henger tett sammen med
"dynamisk tilnærming" under — gjøres det skikkelig bør det ses i sammenheng.

---

## Sidenote: statisk vs. dynamisk årstall/kvartal

Et tilbakevendende mønster i hele kjeden er at **nåværende årstall/kvartal er hardkodet** og
må oppdateres manuelt flere steder hvert kvartal:

- `main.py` i `pia-dvh-import` oppdateres manuelt hvert år.
- Hardkodet i tester.
- `NÅVÆRENDE_KVARTAL` i `pia-sykefravarsstatistikk`.
- `siste_publiseringsinfo` i lydia-api.

**Forslag:** Ett sted som eier "gjeldende årstall/kvartal" og som de andre tjenestene henter
fra — på samme måte som **grunnbeløpet (G)** eksponeres via et felles endepunkt. Da slipper
vi spredte manuelle oppdateringer, og en feil ett sted forplanter seg ikke.

**Estimat:** Dette er **større og uavklart** (et eget designarbeid på tvers av flere repoer).
Bør scopes som egen oppgave før estimering. Som en grov indikasjon: design + implementasjon +
utrulling på tvers av 3–4 tjenester er snakk om **flere dager**, ikke timer. Anbefaling: ta
de små punktene (1–3) først, og løft den dynamiske tilnærmingen som en separat sak.

---

## Oppsummert estimat for del 3

| Oppgave | Estimat | Kommentar |
|---|---|---|
| Forbedre Grafana-logging | ~0,25 dag | Henger sammen med alerts i del 2 |
| Auto-refresh materialized view (lydia-api) | ~1 dag | Kjent SQL, ny trigger-mekanisme |
| Auto-tilgjengeliggjøring (pia-s19k) | ~0,5 dag | Liten endring, men bør ses i sammenheng med dynamisk tilnærming |
| **Sum konkrete oppgaver** | **~1,75 dag** | |
| Dynamisk tilnærming (felles G-lignende endepunkt) | Uavklart, flere dager | Eget designarbeid, scopes separat |

---

## Konkrete forslag til den dynamiske tilnærmingen

Etter en gjennomgang av koden er hovedfunnet at **sannhetskilden allerede finnes** — vi
trenger ikke bygge en ny "G-tjeneste" fra bunnen.

### Hva som finnes i dag

| Tjeneste | Status | Detaljer |
|---|---|---|
| **pia-dvh-import** | Eier dataen | Tabellen `publiseringsdato` (`arstall`, `kvartal`, `offentlig_dato`, `prosessert`, `sist_endret`) i `PubliseringsdatoRepository`. Publiserer allerede på Kafka-topic `KVARTALSVIS_SYKEFRAVARSSTATISTIKK_PUBLISERINGSDATO`. Appen er en Ktor-server, men har kun helse-endepunkter (`isalive`/`isready`). |
| **lydia-api** | Gjør allerede "det riktige" | Konsumerer publiseringsdato fra Kafka → `siste_publiseringsinfo` → utleder gjeldende kvartal og eksponerer `/sykefraværsstatistikk/publiseringsinfo`. Det eneste manuelle er `REFRESH MATERIALIZED VIEW` (punkt 2 over). |
| **pia-sykefravarsstatistikk** | Eneste hardkodede | `NÅVÆRENDE_KVARTAL = ÅrstallOgKvartal(2026, 1)` i `ImporttidspunktRepository`. Konsumerer **ikke** publiseringsdato. Metoden logger selv at verdien er "hardkodet". |
| **main.py (scripts)** | Separat sak | `2024K4` hardkodet i SSB-filnavn/kolonner. Manuell data-prep, ikke del av runtime-flyten. |

### Fire opsjoner

**A — Replikér lydia-api-mønsteret i pia-s19k (anbefalt).**
La pia-s19k konsumere den eksisterende publiseringsdato-Kafka-topicen inn i egen tabell, og
utled gjeldende kvartal dynamisk (siste rad med `prosessert = true` / `offentlig_dato <= i dag`)
i stedet for konstanten. Ingen ny tjeneste, ingen runtime-kobling, og vi gjenbruker et mønster
teamet allerede har i prod. **~1–1,5 dag.**

**B — Nytt HTTP-endepunkt i pia-dvh-import (matcher "G"-modellen).**
pia-dvh-import er allerede en Ktor-server. Legg til `GET /gjeldende-kvartal` som returnerer
`{arstall, kvartal, offentlig_dato}` fra `publiseringsdato`. Konsumentene henter runtime. Mest
lik grunnbeløp-tankegangen, men gir oppetidsavhengighet mellom tjenestene. **~1 dag i
dvh-import + ~0,5 dag per konsument.**

**C — Ren datoberegning fra dagens dato.**
Regn ut "siste publiserte kvartal" fra kalenderen. Skjørt fordi publiseringsdatoene varierer.
Kun egnet som fallback, ikke primærløsning.

**D — Gratis gevinst.**
Punkt 2 (lydia-api auto-refresh) og punkt 3 (pia-s19k) løses i praksis av opsjon A eller B,
siden begge tjenester da leser dynamisk i stedet for hardkodet.

### main.py — hold adskilt

`scripts/main.py` har `2024K4` hardkodet i SSB-filnavn/kolonner. Dette er manuell data-prep
(arbeidsforhold fra SSB), ikke en del av runtime-flyten. Parameteriser årstall/kvartal som
CLI-arg eller env-variabel som leses ett sted. **~0,25 dag.** Bør holdes adskilt fra
tjeneste-refaktoreringen.

### Anbefalt rekkefølge og estimat

| Steg | Hva | Estimat |
|---|---|---|
| 1 | pia-s19k dynamisk via Kafka (opsjon A) — fjern `NÅVÆRENDE_KVARTAL`, oppdater `KafkaContainerHelper` | ~1–1,5 dag |
| 2 | lydia-api auto-refresh (punkt 2) — trigges av samme dynamiske data | ~1 dag |
| 3 | main.py parameterisering | ~0,25 dag |
| 4 | (valgfritt) flytt `ÅrstallOgKvartal` til ia-felles som delt type — fjerner duplisering | ~0,5 dag |

**Konklusjon:** Velg **opsjon A**. Den utnytter eksisterende infrastruktur, kopierer et mønster
som allerede er i prod (lydia-api), og unngår å introdusere en ny tjeneste med egen oppetid.
"G-endepunktet" (opsjon B) er kjekt på sikt dersom flere eksterne forbrukere trenger gjeldende
kvartal, men er overkill for kun pia-s19k.
