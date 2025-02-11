import json
import logging
import os
import random

import pandas as pd
from Virksomhet import Virksomhet

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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


def skriv_virksomheter_til_fil(
    virksomheter: list[Virksomhet],
    årstall: int,
    kvartal: int,
    outputdir: str,
    nasjonalt_sykefravær,
):
    logging.debug(f"Genererer data for {len(virksomheter)} virksomheter")
    virksomhet_json = [
        v.til_virksomhet_json(årstall, kvartal, nasjonalt_sykefravær)
        for v in virksomheter
    ]
    virksomhet_metadata_json = [
        v.til_virksomhet_metadata_json(årstall, kvartal) for v in virksomheter
    ]

    logging.debug(f"skriver data til fil {outputdir}/virksomhet.json")
    skriv_til_fil(data=virksomhet_json, filnavn=f"{outputdir}/virksomhet.json")

    logging.debug(f"skriver data til fil {outputdir}/virksomhet_metadata.json")
    skriv_til_fil(
        data=virksomhet_metadata_json, filnavn=f"{outputdir}/virksomhet_metadata.json"
    )

    logging.debug(f"ferdig med å skrive {len(virksomheter)} virksomheter til filer")


def generer_data(
    årstall: int,
    kvartal: int,
    output_dir: str,
    arbeidsforhold_per_næring,
    arbeidsforhold_per_næringskode,
    arbeidsforhold_per_sektor,
    arbeidsforhold_nasjonalt,
    virksomheter: list[Virksomhet],
    driftsdager,
    gjennomsnittlig_stillingsprosent,
):
    ################################################ publiseringsdato.json ################################################
    publiseringsdato = [
        {
            "rapport_periode": "202403",
            "offentlig_dato": "2024-11-28, 08:00:00",
            "oppdatert_i_dvh": "2023-10-20",
        },
        {
            "rapport_periode": "202402",
            "offentlig_dato": "2024-09-05, 08:00:00",
            "oppdatert_i_dvh": "2023-10-20",
        },
        {
            "rapport_periode": "202401",
            "offentlig_dato": "2024-05-30, 08:00:00",
            "oppdatert_i_dvh": "2023-10-20",
        },
        {
            "rapport_periode": "202304",
            "offentlig_dato": "2024-02-29, 08:00:00",
            "oppdatert_i_dvh": "2023-10-20",
        },
    ]

    # TODO: Skriv publiseringsdato til fil
    # filnavn = f"{output_dir}/{årstall}/publiseringsdato.json"
    # skriv_til_fil(data=publiseringsdato, filnavn=filnavn)

    ################################################ land.json ################################################
    logging.debug("Genererer data for land")

    seed_land = årstall + kvartal

    sykefraværsprosent_nasjonalt = beregn_prosent(seed=seed_land, tall=6.0)

    # TODO over tid vil sysselsatte gå opp og ned med denne utregningen, burde se på forrige kvartal og legge til ?
    antall_personer_nasjonalt = round(
        pluss_minus_prosent(seed=seed_land, tall=arbeidsforhold_nasjonalt)
    )

    mulige_dagsverk_nasjonalt = mulige_dagsverk(
        antall_personer=antall_personer_nasjonalt,
        stillingsprosent=gjennomsnittlig_stillingsprosent,
        driftsdager=driftsdager,
    )

    tapte_dagsverk_nasjonalt = tapte_dagsverk(
        mulige_dagsverk=mulige_dagsverk_nasjonalt,
        sykefraværsprosent=sykefraværsprosent_nasjonalt,
    )

    land_data = [
        {
            "land": "NO",
            "årstall": årstall,
            "kvartal": kvartal,
            "prosent": round(sykefraværsprosent_nasjonalt, 2),
            "tapteDagsverk": round(tapte_dagsverk_nasjonalt, 2),
            "muligeDagsverk": round(mulige_dagsverk_nasjonalt, 2),
            "antallPersoner": antall_personer_nasjonalt,
        }
    ]

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/land.json"
    skriv_til_fil(data=land_data, filnavn=filnavn)

    ################################################ sektor.json ################################################
    logging.debug("Genererer data for sektor")

    sektor_data = []
    sektorer = ["1", "2", "3"]

    for sektor in sektorer:
        seed_sektor = årstall + kvartal + int(sektor)

        antall_personer_sektor = round(
            pluss_minus_prosent(
                seed=seed_sektor, tall=arbeidsforhold_per_sektor[sektor]
            )
        )

        sykefraværsprosent_sektor = beregn_prosent(
            seed=seed_sektor, tall=sykefraværsprosent_nasjonalt
        )

        stillingsprosent_sektor = pluss_minus_prosent(
            seed=seed_sektor,
            tall=gjennomsnittlig_stillingsprosent,
        )

        mulige_dagsverk_sektor = mulige_dagsverk(
            antall_personer=antall_personer_sektor,
            stillingsprosent=stillingsprosent_sektor,
            driftsdager=driftsdager,
        )

        tapte_dagsverk_sektor = tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk_sektor,
            sykefraværsprosent=sykefraværsprosent_sektor,
        )

        ny_sektor = {
            "sektor": sektor,
            "årstall": årstall,
            "kvartal": kvartal,
            "prosent": sykefraværsprosent_sektor,
            "tapteDagsverk": round(tapte_dagsverk_sektor, 2),
            "muligeDagsverk": round(mulige_dagsverk_sektor, 2),
            "antallPersoner": antall_personer_sektor,
        }
        sektor_data.append(ny_sektor)

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/sektor.json"
    skriv_til_fil(data=sektor_data, filnavn=filnavn)

    ################################################ naering.json ################################################
    logging.debug("Genererer data for næring")
    næring_data = []
    for næring in list(arbeidsforhold_per_næring.keys()):
        seed_næring = årstall + kvartal + int(næring)

        sykefraværsprosent_næring = beregn_prosent(
            seed=seed_næring, tall=sykefraværsprosent_nasjonalt
        )

        antall_personer_næring = round(
            pluss_minus_prosent(
                seed=seed_næring, tall=arbeidsforhold_per_næring[næring]
            )
        )
        stillingsprosent_næring = pluss_minus_prosent(
            seed=seed_næring,
            tall=gjennomsnittlig_stillingsprosent,
        )

        mulige_dagsverk_næring = mulige_dagsverk(
            antall_personer=antall_personer_næring,
            stillingsprosent=stillingsprosent_næring,
            driftsdager=driftsdager,
        )

        tapte_dagsverk_næring = tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk_næring,
            sykefraværsprosent=sykefraværsprosent_næring,
        )

        tapte_dagsverk_gradert_næring = (
            tapte_dagsverk_næring * stillingsprosent_næring * 0.5
        )

        tapte_dagsverk_per_varighet = fordel_tapte_dagsverk(
            seed=seed_næring,
            tapte_dagsverk=tapte_dagsverk_næring,
            inkluder_alle_varigheter=True,
        )

        ny_næring = {
            "næring": næring,
            "årstall": årstall,
            "kvartal": kvartal,
            "prosent": sykefraværsprosent_næring,
            "tapteDagsverk": round(tapte_dagsverk_næring, 2),
            "muligeDagsverk": round(mulige_dagsverk_næring, 2),
            "tapteDagsverkGradert": round(tapte_dagsverk_gradert_næring, 2),
            "tapteDagsverkPerVarighet": [
                {"varighet": key, "tapteDagsverk": tapte_dagsverk_per_varighet[key]}
                for key in tapte_dagsverk_per_varighet
            ],
            "antallPersoner": antall_personer_næring,
        }

        næring_data.append(ny_næring)

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/naering.json"
    skriv_til_fil(data=næring_data, filnavn=filnavn)

    ################################################ naeringskode.json ################################################
    logging.debug("Genererer data for næringskode")

    næringskode_data = []
    for næringskode in list(arbeidsforhold_per_næringskode.keys()):
        seed_næringskode = årstall + kvartal + int(næringskode)

        sykefraværsprosent_næringskode = beregn_prosent(
            seed=seed_næringskode, tall=sykefraværsprosent_nasjonalt
        )

        antall_personer_næringskode = round(
            pluss_minus_prosent(
                seed=seed_næringskode, tall=arbeidsforhold_per_næringskode[næringskode]
            )
        )

        stillingsprosent_næringskode = pluss_minus_prosent(
            seed=seed_næringskode,
            tall=gjennomsnittlig_stillingsprosent,
        )

        mulige_dagsverk_næringskode = mulige_dagsverk(
            antall_personer=antall_personer_næringskode,
            stillingsprosent=stillingsprosent_næringskode,
            driftsdager=driftsdager,
        )

        tapte_dagsverk_næringskode = tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk_næringskode,
            sykefraværsprosent=sykefraværsprosent_næringskode,
        )

        tapte_dagsverk_gradert_næringskode = (
            tapte_dagsverk_næringskode * stillingsprosent_næringskode * 0.5
        )

        tapte_dagsverk_per_varighet = fordel_tapte_dagsverk(
            seed=seed_næringskode,
            tapte_dagsverk=tapte_dagsverk_næringskode,
            inkluder_alle_varigheter=True,
        )

        ny_næringskode = {
            "næringskode": næringskode,
            "årstall": årstall,
            "kvartal": kvartal,
            "prosent": sykefraværsprosent_næringskode,
            "tapteDagsverk": round(tapte_dagsverk_næringskode, 2),
            "muligeDagsverk": round(mulige_dagsverk_næringskode, 2),
            "tapteDagsverkGradert": round(tapte_dagsverk_gradert_næringskode, 2),
            # For nøkkel i tapte_dagsverk_per_varighet_næringskode, sett tapteDagsverk til value
            # bruk list comprehension
            "tapteDagsverkPerVarighet": [
                {"varighet": key, "tapteDagsverk": tapte_dagsverk_per_varighet[key]}
                for key in tapte_dagsverk_per_varighet
            ],
            "antallPersoner": antall_personer_næringskode,
        }

        næringskode_data.append(ny_næringskode)

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/naeringskode.json"
    skriv_til_fil(data=næringskode_data, filnavn=filnavn)

    ################################################ Virksomheter ################################################
    logging.debug("Genererer data for virksomheter")
    rectype_1_virksomheter = ["947195360", "944732004", "945071842", "945971401"]

    virksomhet_json = []
    virksomhet_metadata_json = []
    for virksomhet in virksomheter:
        if virksomhet.orgnummer in rectype_1_virksomheter:
            rectype = "1"
        else:
            rectype = "2"

        # Sektor virker å være utledet fra: https://www.ssb.no/klass/klassifikasjoner/39/koder som vi kanskje har i virksomheten
        sektor = str(int(virksomhet.orgnummer) % 3 + 1)
        næring = virksomhet.næringskode.split(".")[0]
        næringskode = virksomhet.næringskode.replace(".", "")

        seed_virksomhet = årstall + kvartal + int(virksomhet.orgnummer)

        sykefraværsprosent_virksomhet = beregn_prosent(
            seed=seed_virksomhet, tall=sykefraværsprosent_nasjonalt, prosent=0.8
        )

        stillingsprosent_virksomhet = pluss_minus_prosent(
            seed=seed_virksomhet,
            tall=gjennomsnittlig_stillingsprosent,
        )

        mulige_dagsverk_virksomhet = mulige_dagsverk(
            antall_personer=virksomhet.antall_ansatte,
            stillingsprosent=stillingsprosent_virksomhet,
            driftsdager=driftsdager,
        )

        tapte_dagsverk_virksomhet = tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk_virksomhet,
            sykefraværsprosent=sykefraværsprosent_virksomhet,
        )

        tapte_dagsverk_gradert_virksomhet = (
            tapte_dagsverk_virksomhet * stillingsprosent_virksomhet * 0.5
        )

        tapte_dagsverk_per_varighet = fordel_tapte_dagsverk(
            seed=seed_virksomhet,
            tapte_dagsverk=tapte_dagsverk_virksomhet,
        )

        ny_data = {
            "orgnr": virksomhet.orgnummer,
            "årstall": årstall,
            "kvartal": kvartal,
            "prosent": sykefraværsprosent_virksomhet,
            "tapteDagsverk": round(tapte_dagsverk_virksomhet, 2),
            "muligeDagsverk": round(mulige_dagsverk_virksomhet, 2),
            "tapteDagsverkGradert": round(tapte_dagsverk_gradert_virksomhet, 2),
            "tapteDagsverkPerVarighet": [
                {"varighet": key, "tapteDagsverk": tapte_dagsverk_per_varighet[key]}
                for key in tapte_dagsverk_per_varighet
            ],
            "antallPersoner": virksomhet.antall_ansatte,
            "rectype": rectype,
        }

        virksomhet_json.append(ny_data)

        ny_metadata = {
            "orgnr": virksomhet.orgnummer,
            "årstall": årstall,
            "kvartal": kvartal,
            "sektor": sektor,
            "primærnæring": næring,
            "primærnæringskode": næringskode,
            "rectype": rectype,
        }

        virksomhet_metadata_json.append(ny_metadata)

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/virksomhet.json"
    skriv_til_fil(data=virksomhet_json, filnavn=filnavn)

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/virksomhet_metadata.json"
    skriv_til_fil(data=virksomhet_metadata_json, filnavn=filnavn)

    logging.debug(f"{årstall}/K{kvartal} ferdig")


def beregn_prosent(seed, tall, prosent=0.1):
    return round(pluss_minus_prosent(seed=seed, tall=tall, prosent=prosent), 1)


def pluss_minus_prosent(seed: int, tall: float, prosent: float = 0.1) -> float:
    random.seed(seed)
    return tall * random.uniform(1 - prosent, 1 + prosent)  # +/- 10%


def skriv_til_fil(data, filnavn):
    logging.debug(f"skriver data til fil: {filnavn}")
    with open(filnavn, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def main():
    gjennomsnittlig_stillingsprosent = 0.85
    # Hent alle arbeidsforhold fra ssb-data
    næringer, næringskoder = hent_næringsgruppering()

    arbeidsforhold_per_næring = hent_arbeidsforhold_næring(næringer=næringer)

    arbeidsforhold_per_næringskode = hent_arbeidsforhold_næringskode(
        næringskoder=næringskoder
    )

    arbeidsforhold_per_sektor = hent_arbeidsforhold_per_sektor()

    arbeidsforhold_nasjonalt = arbeidsforhold_per_sektor["0"]

    # hent virksomheter fra tenor
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

    df = pd.read_csv("data/fia/testvirksomheter.csv", encoding="utf-8")

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
        raise Exception(
            f"Noen virksomheter er i json, men ikke i csv fil: {ikke_i_csv}"
        )

    elif len(ikke_i_json) > 0:
        raise Exception(
            f"Noen virksomheter er i csv-fil, men ikke i json: {ikke_i_json}"
        )

    logging.info(f"Antall næringer: {len(næringer)}")

    logging.info(f"Antall næringskoder: {len(næringskoder)}")

    logging.info(f"Antall virksomheter: {len(virksomheter)}")

    logging.info(
        f"Antar en gjennomsnittlig stillingsprosent på {gjennomsnittlig_stillingsprosent * 100}%"
    )

    driftsdager_per_kvartal = {1: 63, 2: 64, 3: 66, 4: 63}

    # generer data for årstall og kvartal
    for årstall in range(2020, 2025):
        for kvartal in [1, 2, 3, 4]:
            # Lag output mappe
            output_dir = "output"
            kvartal_dir = f"{output_dir}/{årstall}/K{kvartal}"
            if not os.path.exists(kvartal_dir):
                os.makedirs(kvartal_dir)

            logging.info(f"Generer data for årstall: {årstall} og kvartal: {kvartal}")

            # Generer data
            generer_data(
                årstall=årstall,
                kvartal=kvartal,
                output_dir=output_dir,
                arbeidsforhold_per_næring=arbeidsforhold_per_næring,
                arbeidsforhold_per_næringskode=arbeidsforhold_per_næringskode,
                arbeidsforhold_per_sektor=arbeidsforhold_per_sektor,
                arbeidsforhold_nasjonalt=arbeidsforhold_nasjonalt,
                virksomheter=virksomheter,
                driftsdager=driftsdager_per_kvartal[kvartal],
                gjennomsnittlig_stillingsprosent=gjennomsnittlig_stillingsprosent,
            )


def mulige_dagsverk(
    antall_personer: int, stillingsprosent: float, driftsdager: int
) -> float:
    return antall_personer * stillingsprosent * driftsdager


def tapte_dagsverk(mulige_dagsverk: float, sykefraværsprosent: float) -> float:
    return mulige_dagsverk * sykefraværsprosent / 100


def hent_næringsgruppering(
    path: str = "data/ssb/alle_næringskoder.csv",
) -> tuple[list[str], list[str]]:
    # Henter alle næringer og næringskoder fra https://www.ssb.no/klass/klassifikasjoner/6

    df = pd.read_csv(path, encoding="unicode_escape", sep=";")

    # Alle næringer
    næringer = df.loc[df["level"] == 2]
    næringer = næringer["code"].unique().tolist()

    # Alle næringskoder
    næringskoder = df.loc[df["level"] == 5]
    næringskoder = næringskoder["code"].unique().tolist()
    næringskoder = [næringskode.replace(".", "") for næringskode in næringskoder]
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


def fordel_tapte_dagsverk(
    seed,
    tapte_dagsverk,
    inkluder_alle_varigheter=False,
):
    random.seed(seed)
    varigheter = ["A", "B", "C", "D", "E", "F"]

    if not inkluder_alle_varigheter:
        # drop tilfeldig antall elementer
        for _ in range(random.randint(1, len(varigheter) - 1)):
            # drop tilfeldig element fra varigheter
            varigheter.pop(random.randint(0, len(varigheter) - 1))

    distribution = {varighet: 0.0 for varighet in varigheter}

    # Distribute the tapte_dagsverk randomly
    remaining_dagsverk = tapte_dagsverk
    for i in range(len(varigheter) - 1):
        # Randomly decide the amount to distribute to the current varighet
        amount = round(random.uniform(0, remaining_dagsverk), 2)
        distribution[varigheter[i]] += amount
        remaining_dagsverk -= amount

    # Assign the remaining amount to the last varighet
    distribution[varigheter[-1]] += round(remaining_dagsverk, 2)

    return distribution


if __name__ == "__main__":
    main()

    # TODO: Hvis vi tar utgangspunkt i ekte antall arbeidsforhold per næring, næringskode, sektor
    # så kan vi generere sykefravær basert på dette. Det kan føre til et problem om vi har en virksomhet som det ikke er arbeidsforhold for. f.eks. dyrking av ris
