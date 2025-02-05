import json
import logging
import os

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
    logging.debug(f"genererer data for {len(virksomheter)} virksomheter")
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


def main(årstall: int, kvartal: int):
    # TODO: Sjekk for duplikater:
    # verifiser at alle virksomheter er i testvirksomheter.csv
    # verifiser at alle virksomheter i testvirksomheter.csv er i TENOR filene

    # Opprett outputdir/årstall/kvartal
    output_dir = "output"
    kvartal_dir = f"{output_dir}/{årstall}/{kvartal}"
    if not os.path.exists(kvartal_dir):
        os.makedirs(kvartal_dir)

    ###### årstall og kvartal
    logging.info(f"Generer data for årstall: {årstall} og kvartal: {kvartal}")

    ###### publiseringsdato.json
    logging.info("TODO: Generer data for publiseringsdato dynamisk")

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

    filnavn = f"{output_dir}/{årstall}/publiseringsdato.json"
    skriv_til_fil(data=publiseringsdato, filnavn=filnavn)

    ###### land.json
    logging.info("genererer data for land (TODO: generer dynamisk)")

    # TODO
    sykefraværsprosent_nasjonalt = -1000  # 6.0

    land_data = {
        "land": "NO",
        "årstall": årstall,
        "kvartal": kvartal,
        "prosent": sykefraværsprosent_nasjonalt,
        # Tapte dagsverk = 504339.8
        "tapteDagsverk": -1000,  # TODO
        # Mulige dagsverk = 1.01048491
        "muligeDagsverk": -1000,  # TODO
        # Antall personer = 3000001 (hvor mange i arbeid ?)
        "antallPersoner": -1000,  # TODO
    }

    filnavn = f"{output_dir}/{årstall}/{kvartal}/land.json"
    skriv_til_fil(data=land_data, filnavn=filnavn)

    ###### sektor.json
    logging.info("genererer data for sektor (TODO: generer dynamisk)")

    # Statlig forvaltning -> "1"
    # Kommunal forvaltning -> "2"
    # Privat og offentlig næringsvirksomhet -> "3"
    # Fylkeskommunal forvaltning -> "9" # TODO: Ikke i bruk?

    sektor = "1"
    sykefraværsprosent_sektor = -1000  # TODO

    sektor_data = {
        "sektor": sektor,  # TODO
        "årstall": årstall,
        "kvartal": kvartal,
        "prosent": sykefraværsprosent_sektor,
        "tapteDagsverk": -1000,  # TODO
        "muligeDagsverk": -1000,  # TODO
        "antallPersoner": -1000,  # TODO
    }
    filnavn = f"{output_dir}/{årstall}/{kvartal}/sektor.json"
    skriv_til_fil(data=sektor_data, filnavn=filnavn)

    ###### naering.json
    logging.info("genererer data for næring (TODO: generer dynamisk)")

    sykefraværsprosent_næring = -1000  # TODO

    næring = {
        "næring": "22",
        "årstall": årstall,
        "kvartal": kvartal,
        "prosent": sykefraværsprosent_næring,
        "tapteDagsverk": -1000,  # TODO
        "muligeDagsverk": -1000,  # TODO
        "tapteDagsverkGradert": -1000,  # TODO
        "tapteDagsverkPerVarighet": [
            {
                "varighet": "A",
                "tapteDagsverk": -1000,  # TODO
            },
            {
                "varighet": "C",
                "tapteDagsverk": -1000,  # TODO
            },
        ],
        "antallPersoner": -1000,  # TODO
    }
    filnavn = f"{output_dir}/{årstall}/{kvartal}/naering.json"
    skriv_til_fil(data=næring, filnavn=filnavn)

    ###### naeringskode.json
    logging.info("genererer data for næringskode (TODO: generer dynamisk)")

    sykefraværsprosent_næringskode = -1000  # TODO

    næringskode = {
        "næringskode": "88911",
        "årstall": årstall,
        "kvartal": kvartal,
        "prosent": sykefraværsprosent_næringskode,
        "tapteDagsverk": -1000,  # TODO
        "muligeDagsverk": -1000,  # TODO
        "tapteDagsverkGradert": -1000,  # TODO
        "tapteDagsverkPerVarighet": [
            {
                "varighet": "A",
                "tapteDagsverk": -1000,  # TODO
            },
            {
                "varighet": "C",
                "tapteDagsverk": -1000,  # TODO
            },
        ],
        "antallPersoner": -1000,  # TODO
    }

    filnavn = f"{output_dir}/{årstall}/{kvartal}/naeringskode.json"
    skriv_til_fil(data=næringskode, filnavn=filnavn)

    ###### Virksomheter
    logging.info("genererer data for virksomheter")
    # TODO: Les filer og generer data for år og kvartal, ikke les alle filer hvert år og kvartal

    tenor_filer = []
    for path, subdirs, files in os.walk("data/tenor"):
        for name in files:
            filnavn = os.path.join(path, name)
            tenor_filer.append(filnavn)
            logging.debug(f"leser fil: {filnavn}")

    alle_virksomheter = []
    for filnavn in tenor_filer:
        alle_virksomheter.extend(parse_tenor_fil(filnavn))

    skriv_virksomheter_til_fil(
        virksomheter=alle_virksomheter,
        årstall=årstall,
        kvartal=kvartal,
        outputdir=f"{output_dir}/{årstall}/{kvartal}",
        nasjonalt_sykefravær=sykefraværsprosent_nasjonalt,
    )


def skriv_til_fil(data, filnavn):
    with open(filnavn, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    for årstall in range(1992, 1993):
        for kvartal in [1, 2, 3, 4]:
            main(årstall=årstall, kvartal=kvartal)
