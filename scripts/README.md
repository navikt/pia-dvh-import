# Opprett testdata med Tenor

## Hent testdata

Logg inn med egen BankId på [Tenor](https://testdata.skatteetaten.no/web/testnorge/soek/freg)

Naviger til [avansert søk med KQL](https://testdata.skatteetaten.no/web/testnorge/avansert/brreg-er-fr)

Lim inn f.eks. følgende søk i søkefeltet:

```erUnderenhet : * and naeringKode : 86* and antallAnsatte>= 5```

I eksempelet over ønsker vi å hente en liste av underenheter med næringskode `86.***` som har flere enn 5 ansatte.

## Eksport testdata til JSON

På høyresiden av KQL søkefeltet klick på de tre prikkene. I dialogboksen velg "Eksportere resultat"

I neste vindu angi/velg:

- antall bedrifter du vil eksportere
- hukk av check-box "all informasjon inkludert tilknyttende kilder"

Ta vare på filen som lastes ned på din maskin, og legg i rett mappe:
```scripts/data/tenor/fylke/primærnæring/din_nye_fil.json```

## Generer testdata
oppdater avhengigheter med:
```bash
uv sync
```
for å laste ned avhengigheter, og deretter
```bash
uv run main.py
```
for å generere test filer

TODO: Skriv mer om hvordan