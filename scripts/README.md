# Opprett testdata med Tenor

1. Følg [bruksanvisning](https://github.com/navikt/lydia-api/blob/main/README.md#legge-til-nye-testvirksomheter-i-dev) for å søke etter og opprette virksomheter til testdata i Fia (lydia-api)

2. Sørg for at alle resultater av tenor-testdata søk ligger i rett mappe. Scriptet forventer en mappe med flere json filer som inneholder resultater av søk i Tenor Testdata. Du bør sørge for at vi har samme data både i [lydia-api](https://github.com/navikt/lydia-api/tree/main/scripts/tenor/data) og i [pia-dvh-import](https://github.com/navikt/pia-dvh-import/tree/main/scripts/data/tenor).

3. Kjør script for å lese fra denne og generere nye filer med sykefraværsstatistikk.

4. Last opp filer for nytt kvartal i bucket.

5. Kjør import i pia-jobbsender for å importere data til dev-miljø og eksportere til Fia og pia-sykefraværsstatistikk.