import json
import logging
import os

import pandas as pd
from Virksomhet import Virksomhet

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def last_virksomheter():
    logging.debug("Leser virksomheter fra filer")

    tenor_filer = []
    for path, subdirs, files in os.walk("data/tenor"):
        for name in files:
            filnavn = os.path.join(path, name)
            tenor_filer.append(filnavn)
            logging.debug(f"leser fil: {filnavn}")

    virksomheter = []
    for filnavn in tenor_filer:
        virksomheter.extend(parse_tenor_fil(filnavn))

    virksomheter = list(set(virksomheter))
    logging.debug(f"Antall virksomheter fra tenor: {len(virksomheter)}")

    logging.debug("Sjekker virksomheter fra tenor opp mot csv-fil")
    df = pd.read_csv("data/fia/testvirksomheter.csv", encoding="utf-8")
    logging.debug(f"Antall virksomheter i csv-fil: {df.shape[0]}")

    ikke_i_json = [
        orgnr
        for orgnr in df["orgnr"].unique().tolist()
        if orgnr not in [int(v.orgnummer) for v in virksomheter]
    ]

    ikke_i_csv = [
        orgnr
        for orgnr in [int(v.orgnummer) for v in virksomheter]
        if orgnr not in df["orgnr"].unique().tolist()
    ]

    if len(ikke_i_csv) > 0:
        logging.error(f"Noen virksomheter er i json, men ikke i csv fil: {ikke_i_csv}")
        raise Exception(
            f"Noen virksomheter er i json, men ikke i csv fil: {ikke_i_csv}"
        )
    elif len(ikke_i_json) > 0:
        logging.error(f"Noen virksomheter er i csv-fil, men ikke i json: {ikke_i_json}")
        raise Exception(
            f"Noen virksomheter er i csv-fil, men ikke i json: {ikke_i_json}"
        )
    else:
        logging.info(f"Antall virksomheter lastet inn: {len(virksomheter)}")
        return virksomheter


def les_fra_filer():
    næringer, næringskoder = hent_næringsgruppering()
    arbeidsforhold_næring = hent_arbeidsforhold_næring(næringer=næringer)
    arbeidsforhold_næringskode = hent_arbeidsforhold_næringskode(
        næringskoder=næringskoder
    )

    arbeidsforhold_per_sektor = hent_arbeidsforhold_per_sektor()

    arbeidsforhold_nasjonalt = arbeidsforhold_per_sektor["0"]

    arbeidsforhold_per_sektor.pop("0")

    return (
        næringer,
        næringskoder,
        arbeidsforhold_næring,
        arbeidsforhold_næringskode,
        arbeidsforhold_per_sektor,
        arbeidsforhold_nasjonalt,
    )


def skriv_til_fil(data, filnavn):
    logging.debug(f"skriver data til fil: {filnavn}")
    with open(filnavn, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def hent_arbeidsforhold_næringskode(næringskoder: list[str]) -> dict:
    # Henter antall arbeidsforhold fra SSB https://www.ssb.no/statbank/table/13926 for 2024K4
    # Sett bort ifra næringskoder som ikke er i https://www.ssb.no/klass/klassifikasjoner/6
    # Disse er:
    # "00.000", uoppgitt
    # "01.000u", Jordbruk, tilhør.tjenesterm jakt, ufordelt
    # "02.000u", skogbruk og tiløhrende tjenster, ufordelt
    # "03.000u", Fiske, fangst og akvakultur, ufordelt
    # "43.220", VVS-arbeid
    # "52.215", Tjenester tilknyttet drift av rørledninger
    # "55.301", Drift av campingplasser (Drift av campingplasser 55.300 er inkludert)
    # "55.302", Drift av turisthytter
    # "64.303", Porteføljeinvesteringsselskaper
    # "64.304", Skattebetingede investeringsselskaper
    # "64.309", Annen verdipapirvirksomhet

    df = pd.read_csv(
        "data/ssb/arbeidsforhold_etter_næringskode_2024K4.csv",
        encoding="unicode_escape",
        sep=";",
        header=1,
    )

    df["næringskode"] = (
        df["næring (SN2007)"].str.split(" ", n=1, expand=True)[0].str.replace(".", "")
    )

    ikke_i_standard = [
        næringskode
        for næringskode in df["næringskode"].to_list()
        if næringskode not in næringskoder
    ]

    ikke_i_tabell = [
        næringskode
        for næringskode in næringskoder
        if næringskode not in df["næringskode"].to_list()
    ]

    if len(ikke_i_tabell) > 0:
        raise Exception(
            f"Noen næringskoder fra næringsgruppering mangler i ssb tabell: {ikke_i_tabell}"
        )
    elif len(ikke_i_standard) > 0:
        raise Exception(
            f"Noen næringskoder fra ssb tabell mangler i næringsgruppering: {ikke_i_standard}"
        )
    else:
        logging.info("Alle næringskoder er i næringsgruppering og ssb tabell")

    return df.set_index("næringskode")[
        "Antall jobber (arbeidsforhold) 2024K4 0 Hele landet"
    ].to_dict()


def hent_arbeidsforhold_per_sektor() -> dict:
    # Henter antall arbeidsforhold fra SSB https://www.ssb.no/statbank/table/11653/ for 2024K4
    df = pd.read_csv(
        "data/ssb/arbeidsforhold_etter_sektor_2024K4.csv",
        encoding="unicode_escape",
        sep=";",
        header=1,
    )

    # "Privat sektor, offentlig eide foretak og uoppgitt"  # 3
    # "Kommuneforvaltningen"  # 2
    # "Statsforvaltningen"  # 1
    # "Sum alle sektorer"  # 0 (Nasjonalt)

    # Gjør om kolonne i dataframe til tall:
    df["sektor"].unique()
    # ['Sum alle sektorer', 'Privat sektor, offentlig eide foretak og uoppgitt', 'Kommuneforvaltningen', 'Statsforvaltningen']
    # ['0', '3', '2', '1']
    df["sektor"] = df["sektor"].replace(
        {
            "Sum alle sektorer": "0",
            "Privat sektor, offentlig eide foretak og uoppgitt": "3",
            "Kommuneforvaltningen": "2",
            "Statsforvaltningen": "1",
        }
    )

    return df.set_index("sektor")["Antall jobber (arbeidsforhold) 2024K4"].to_dict()


def parse_tenor_fil(
    import_filename: str,
) -> list[Virksomhet]:
    nye_virksomheter = []

    with open(import_filename, "r") as tenorTestDataFil:
        json_decode = json.load(tenorTestDataFil)
        for virksomhet in json_decode["dokumentListe"]:
            nye_virksomheter.append(
                Virksomhet(
                    orgnummer=virksomhet.get("organisasjonsnummer"),
                    navn=virksomhet.get("navn"),
                    næringskode=virksomhet.get("naeringKode")[0],
                    kommune=virksomhet.get("forretningsadresse").get("kommune"),
                    postnummer=virksomhet.get("forretningsadresse").get("postnummer"),
                    adresse_lines=[
                        adresse.replace("'", "")
                        for adresse in virksomhet.get("forretningsadresse").get(
                            "adresse"
                        )
                    ],
                    kommunenummer=virksomhet.get("forretningsadresse").get(
                        "kommunenummer"
                    ),
                    poststed=virksomhet.get("forretningsadresse").get("poststed"),
                    landkode=virksomhet.get("forretningsadresse").get("landkode"),
                    land=virksomhet.get("forretningsadresse").get("land"),
                    oppstartsdato=virksomhet.get("registreringsdatoEnhetsregisteret"),
                    antall_ansatte=virksomhet.get("antallAnsatte"),
                )
            )
    return nye_virksomheter


def hent_næringsgruppering(
    path: str = "data/ssb/alle_næringskoder.csv",
) -> tuple[list[str], list[str]]:
    # Henter alle næringer og næringskoder fra https://www.ssb.no/klass/klassifikasjoner/6

    df = pd.read_csv(path, encoding="unicode_escape", sep=";")

    # Alle næringer
    næringer = df.loc[df["level"] == 2]
    næringer = næringer["code"].unique().tolist()
    logging.info(f"Antall næringer lastet inn: {len(næringer)}")

    # Alle næringskoder
    næringskoder = df.loc[df["level"] == 5]
    næringskoder = næringskoder["code"].unique().tolist()
    næringskoder = [næringskode.replace(".", "") for næringskode in næringskoder]
    logging.info(f"Antall næringskoder lastet inn: {len(næringskoder)}")
    return næringer, næringskoder


def hent_arbeidsforhold_næring(næringer: list[str]) -> dict:
    # Henter antall arbeidsforhold fra SSB https://www.ssb.no/statbank/table/11656 for 2024K4
    df = pd.read_csv(
        "data/ssb/arbeidsforhold_etter_næring_2024K4.csv",
        encoding="unicode_escape",
        sep=";",
        header=1,
    )
    # beholder bare første tallet og ikke beskrivelsen
    df["næring"] = df["næring (SN2007)"].str.split(" ", n=1, expand=True)[0]

    ikke_i_standard = [
        næring for næring in df["næring"].to_list() if næring not in næringer
    ]

    ikke_i_tabell = [
        næring for næring in næringer if næring not in df["næring"].to_list()
    ]

    if len(ikke_i_tabell) > 0:
        raise Exception(
            f"Noen næringer fra næringsgruppering mangler i ssb tabell: {ikke_i_tabell}"
        )
    elif len(ikke_i_standard) > 0:
        raise Exception(
            f"Noen næringer fra ssb tabell mangler i næringsgruppering: {ikke_i_standard}"
        )
    else:
        logging.info("Alle næringer er i næringsgruppering og ssb tabell")

    return df.set_index("næring")[
        "Antall jobber (arbeidsforhold) 2024K4 Begge kjønn"
    ].to_dict()
