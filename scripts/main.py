import json
import logging
import os
from dataclasses import asdict
from typing import Any

import pandas as pd
from Statistikk import (
    LandStatistikk,
    NæringskodeStatistikk,
    NæringStatistikk,
    SektorStatistikk,
    VirksomhetMetaDataStatistikk,
    VirksomhetStatistikk,
    varier_med_prosent,
)
from Virksomhet import Virksomhet, les_virksomheter_fra_fil

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def hent_arbeidsforhold_næringskode(
    filnavn: str = "data/ssb/arbeidsforhold_etter_næringskode_2024K4.csv",
) -> dict[str, int]:
    """
    Henter antall arbeidsforhold fra SSB https://www.ssb.no/statbank/table/13926 for 2024K4
    Sett bort ifra næringskoder som ikke er i https://www.ssb.no/klass/klassifikasjoner/6
    Disse er:
    "00.000", uoppgitt
    "01.000u", Jordbruk, tilhør.tjenesterm jakt, ufordelt
    "02.000u", skogbruk og tiløhrende tjenster, ufordelt
    "03.000u", Fiske, fangst og akvakultur, ufordelt
    "43.220", VVS-arbeid
    "52.215", Tjenester tilknyttet drift av rørledninger
    "55.301", Drift av campingplasser (Drift av campingplasser 55.300 er inkludert)
    "55.302", Drift av turisthytter
    "64.303", Porteføljeinvesteringsselskaper
    "64.304", Skattebetingede investeringsselskaper
    "64.309", Annen verdipapirvirksomhet

    :param filnavn: Filnavn for CSV-filen som inneholder arbeidsforhold
    :type filnavn: str
    :return: Dictionary med næringskoder som nøkler og antall arbeidsforhold som verdier
    :rtype: dict[str, int]
    """

    # TODO: Dette må sikkert oppdateres til næringskode SN2025 når vi får ny statistikk på det formatet

    df: pd.DataFrame = pd.read_csv(
        filnavn,
        encoding="unicode_escape",
        sep=";",
        header=1,
    )

    df["næringskode"] = (
        df["næring (SN2007)"].str.split(" ", n=1, expand=True)[0].str.replace(".", "")
    )

    return df.set_index("næringskode")[
        "Antall jobber (arbeidsforhold) 2024K4 0 Hele landet"
    ].to_dict()


def hent_arbeidsforhold_næring(
    filnavn: str = "data/ssb/arbeidsforhold_etter_næring_2024K4.csv",
) -> dict[str, int]:
    """
    Henter antall arbeidsforhold fra SSB https://www.ssb.no/statbank/table/11656 for 2024K4

    :param filnavn: Filnavn for CSV-filen som inneholder arbeidsforhold
    :type filnavn: str
    :return: Dictionary med næring som nøkler og antall arbeidsforhold som verdier, f.eks. "01": 100, "02": 3000 ...
    :rtype: dict[str, int]
    """

    # TODO: Dette må sikkert oppdateres til næringskode SN2025 når vi får ny statistikk på det formatet

    df: pd.DataFrame = pd.read_csv(
        filnavn,
        encoding="unicode_escape",
        sep=";",
        header=1,
    )
    df["næring"] = df["næring (SN2007)"].str.split(" ", n=1, expand=True)[0]

    return df.set_index("næring")[
        "Antall jobber (arbeidsforhold) 2024K4 Begge kjønn"
    ].to_dict()


def hent_arbeidsforhold_per_sektor(
    filnavn: str = "data/ssb/arbeidsforhold_etter_sektor_2024K4.csv",
) -> dict[str, int]:
    """
    Henter antall arbeidsforhold fra fil basert på data fra SSB https://www.ssb.no/statbank/table/11653/ for 2024K4

    :param filnavn: Filnavn for CSV-filen som inneholder arbeidsforhold
    :type filnavn: str
    :return: Dictionary med sektor ["0", "1", "2", "3"] som nøkler og antall arbeidsforhold som verdier. "0" er totalt for Norge.
    :rtype: dict[str, int]
    """

    df: pd.DataFrame = pd.read_csv(
        filnavn,
        encoding="unicode_escape",
        sep=";",
        header=1,
    )
    # Gjør om kolonne i dataframe til tall sektorkode:
    df["sektor"] = df["sektor"].replace(
        {
            "Sum alle sektorer": "0",
            "Privat sektor, offentlig eide foretak og uoppgitt": "3",
            "Kommuneforvaltningen": "2",
            "Statsforvaltningen": "1",
        }
    )

    return df.set_index("sektor")["Antall jobber (arbeidsforhold) 2024K4"].to_dict()


def skriv_til_fil(data: list[dict[str, Any]], filnavn: str) -> None:
    logging.debug(f"skriver data til fil: {filnavn}")
    with open(filnavn, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    # TODO: Hvis vi tar utgangspunkt i ekte antall arbeidsforhold per næring, næringskode, sektor
    # så kan vi generere sykefravær basert på dette. Det kan føre til et problem om vi har en virksomhet som det ikke er arbeidsforhold for. f.eks. dyrking av ris
    input_dir: str = "data/tenor"
    output_dir: str = "output"
    tenor_filer: list[str] = [
        f"{input_dir}/{filnavn}"
        for filnavn in os.listdir(input_dir)
        if filnavn != ".DS_Store"
    ]

    virksomheter: set[Virksomhet] = {
        virksomhet
        for filnavn in tenor_filer
        for virksomhet in les_virksomheter_fra_fil(filnavn=filnavn)
    }

    logging.info(f"Leste {len(virksomheter)} unike virksomheter fra Tenor data")

    df_ssb_næringer: pd.DataFrame = pd.read_csv(
        "data/ssb/alle_næringskoder.csv",
        encoding="unicode_escape",
        sep=";",
    )

    # Alle næringer
    næringer_df: pd.DataFrame = df_ssb_næringer.loc[df_ssb_næringer["level"] == 2]
    næringer: list[str] = (
        næringer_df["code"].unique().tolist()
    )  # TODO: Finn ut av denne i egen fil
    logging.info(f"Antall næringer lastet inn: {len(næringer)}")

    # Alle næringskoder
    næringskoder_df: pd.DataFrame = df_ssb_næringer.loc[df_ssb_næringer["level"] == 5]
    næringskoder: list[str] = næringskoder_df["code"].unique().tolist()
    næringskoder = [næringskode.replace(".", "") for næringskode in næringskoder]
    logging.info(f"Antall næringskoder lastet inn: {len(næringskoder)}")

    arbeidsforhold_per_næring: dict[str, int] = hent_arbeidsforhold_næring()
    arbeidsforhold_per_næringskode: dict[str, int] = hent_arbeidsforhold_næringskode()

    arbeidsforhold_per_sektor: dict[str, int] = hent_arbeidsforhold_per_sektor()
    arbeidsforhold_norge: int = arbeidsforhold_per_sektor["0"]
    arbeidsforhold_per_sektor.pop("0")

    stillingsprosent_norge: float = 0.85
    driftsdager_per_kvartal: dict[int, int] = {1: 63, 2: 64, 3: 66, 4: 63}

    # generer data for årstall og kvartal
    for årstall in range(2025, 2026):
        for kvartal in [1, 2, 3, 4]:
            # Lag output mappe
            kvartal_dir = f"{output_dir}/{årstall}/K{kvartal}"
            if not os.path.exists(kvartal_dir):
                os.makedirs(kvartal_dir)

            logging.info(f"Generer data for årstall: {årstall} og kvartal: {kvartal}")

            prosent_norge: float = varier_med_prosent(
                seed=årstall + kvartal, tall=6.0, antall_desimaler=1
            )

            # Land

            land_data = LandStatistikk(
                årstall=årstall,
                kvartal=kvartal,
                prosent_norge=prosent_norge,
                arbeidsforhold_norge=arbeidsforhold_norge,
                stillingsprosent_norge=stillingsprosent_norge,
                driftsdager=driftsdager_per_kvartal[kvartal],
            )

            skriv_til_fil(
                data=[asdict(land_data)],
                # ^ tar inn en liste av dict og for kategorien Land er det bare ett land med data
                filnavn=f"{output_dir}/{årstall}/K{kvartal}/land.json",
            )

            # Sektor

            sektor_data: list[SektorStatistikk] = [
                SektorStatistikk(
                    årstall=årstall,
                    kvartal=kvartal,
                    prosent_norge=prosent_norge,
                    sektor=sektor,
                    arbeidsforhold=arbeidsforhold_per_sektor[sektor],
                    stillingsprosent_norge=stillingsprosent_norge,
                    driftsdager=driftsdager_per_kvartal[kvartal],
                )
                for sektor in ["1", "2", "3"]
            ]

            skriv_til_fil(
                data=[asdict(data) for data in sektor_data],
                filnavn=f"{output_dir}/{årstall}/K{kvartal}/sektor.json",
            )

            # Næring

            næringer: list[str] = list(arbeidsforhold_per_næring.keys())

            næring_data: list[NæringStatistikk] = [
                NæringStatistikk(
                    årstall=årstall,
                    kvartal=kvartal,
                    prosent_norge=prosent_norge,
                    næring=næring,
                    arbeidsforhold=arbeidsforhold_per_næring[næring],
                    stillingsprosent_norge=stillingsprosent_norge,
                    driftsdager=driftsdager_per_kvartal[kvartal],
                )
                for næring in næringer
            ]

            filnavn = f"{output_dir}/{årstall}/K{kvartal}/naering.json"
            skriv_til_fil(data=[asdict(data) for data in næring_data], filnavn=filnavn)

            # Næringskode

            næringskode_data: list[NæringskodeStatistikk] = [
                NæringskodeStatistikk(
                    årstall=årstall,
                    kvartal=kvartal,
                    prosent_norge=prosent_norge,
                    næringskode=næringskode,
                    arbeidsforhold=arbeidsforhold_per_næringskode[næringskode],
                    stillingsprosent_norge=stillingsprosent_norge,
                    driftsdager=driftsdager_per_kvartal[kvartal],
                )
                for næringskode in list(arbeidsforhold_per_næringskode.keys())
            ]

            filnavn = f"{output_dir}/{årstall}/K{kvartal}/naeringskode.json"
            skriv_til_fil(
                data=[asdict(data) for data in næringskode_data], filnavn=filnavn
            )

            # Virksomhet
            virksomhet_data: list[VirksomhetStatistikk] = [
                VirksomhetStatistikk(
                    årstall=årstall,
                    kvartal=kvartal,
                    prosent_norge=prosent_norge,
                    virksomhet=virksomhet,
                    stillingsprosent_norge=stillingsprosent_norge,
                    driftsdager=driftsdager_per_kvartal[kvartal],
                )
                for virksomhet in virksomheter
            ]

            filnavn = f"{output_dir}/{årstall}/K{kvartal}/virksomhet.json"
            skriv_til_fil(
                data=[asdict(data) for data in virksomhet_data], filnavn=filnavn
            )

            # Virksomhet Metadata
            virksomhet_metadata: list[VirksomhetMetaDataStatistikk] = [
                VirksomhetMetaDataStatistikk(
                    årstall=årstall,
                    kvartal=kvartal,
                    virksomhet=virksomhet,
                )
                for virksomhet in virksomheter
            ]

            filnavn = f"{output_dir}/{årstall}/K{kvartal}/virksomhet_metadata.json"
            skriv_til_fil(
                data=[asdict(data) for data in virksomhet_metadata], filnavn=filnavn
            )
