# Automatisering av DVH-import — oversikt

Denne mappen dokumenterer arbeidet med å automatisere den kvartalsvise importen av
sykefraværsstatistikk fra DVH (Datavarehus). Dokumentasjonen er delt i tre deler pluss
denne overordnede introen.

## Hvorfor automatisere?

I dag er den kvartalsvise importen en **manuell, personavhengig** prosess som **må gjøres
på selve publiseringsdagen**. Vi er avhengige av Thomas for å kjøre den.

- **Risiko:** feil ble tidligere slukt stille, og det fantes ingen måte å vite om noe gikk
  galt underveis.
- **Manuelle kvalitetssjekker** i dag er alltid mindre sikre enn automatiske sjekker.

## DVH-import: de tre stegene (vårt ansvar)

Hele dataflyten fra DVH og ut til konsumentene går i tre steg:

```
1. Import            2. Eksport                   3. Import
pia-dvh-import  -->  pia-sykefravarsstatistikk -->  lydia-api / Salesforce / FAGER
```

| Steg | System | Ansvar | Automatisering |
|---|---|---|---|
| 1. **Import** | `pia-dvh-import` | Les statistikk fra GCS, valider, send til Kafka | Hovedfokus nå (del 1 + del 2) |
| 2. **Eksport** | `pia-sykefravarsstatistikk` | Tilgjengeliggjør siste importerte kvartal | Manuelt i dag (del 3, lav prio) |
| 3. **Import** | `lydia-api`, Salesforce, FAGER | Konsumer og bruk dataen | `lydia-api` materialized view manuelt i dag (del 3, lav prio) |

Vi jobber nå hovedsakelig med **steg 1** (`pia-dvh-import`), pluss én oppgave på
materialized view i `lydia-api`.

## Dokumentstruktur

| Del | Fil | Innhold | Status |
|---|---|---|---|
| **Del 1** | [del-1-grunnautomatisering.md](del-1-grunnautomatisering.md) | Daglige CronJobs, dry-run, feilhåndtering, `startFra`-parameter, DB, alerts | I hovedsak ferdig |
| **Del 2** | [del-2-orkestrering-validering-alerts.md](del-2-orkestrering-validering-alerts.md) | Orkestrering, validering av input-data, Slack-alerts | Gjenstår — ~10 dager |
| **Del 3** | [del-3-dynamisk-tilnaerming.md](del-3-dynamisk-tilnaerming.md) | Dynamisk årstall/kvartal, auto-tilgjengeliggjøring, Grafana | Lav prioritet / senere |

> Den opprinnelige samledokumentasjonen ligger fortsatt i
> [../../AUTOMATISERING.md](../../AUTOMATISERING.md) og brukes som detaljert endringslogg for del 1.

## Live: 3. september 2026

Neste publiseringsdato er **3. september 2026**. Målet er at **alt med høy prioritet
(del 1 + del 2) skal være automatisert** innen da. Oppgavene med lav prioritet (del 3) kan
tas senere.

## Overordnet mål

Automatisere hele prosessen slik at importen kjører selv på publiseringsdagen, med god
feilhåndtering, kvalitetssikring av data **før** den sendes videre, og varsling til alle
interessenter.

**Viktigste krav:** Importen må **stoppe ved feil og ikke starte på nytt automatisk**.
Gjenopptak skjer kontrollert fra riktig sted, slik at vi ikke sender samme data flere ganger.
