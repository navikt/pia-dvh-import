import logging
import os

from les_og_skriv_til_fil import last_virksomheter, les_fra_filer, skriv_til_fil
from testdata_generator import (
    generer_land,
    generer_næring,
    generer_næringskode,
    generer_publiseringsdato,
    generer_sektor,
    generer_virksomhet,
)
from utregninger import (
    beregn_sykefraværsprosent,
)
from Virksomhet import Virksomhet

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def generer_data(
    årstall: int,
    kvartal: int,
    output_dir: str,
    arbeidsforhold_per_næring,
    arbeidsforhold_per_næringskode,
    arbeidsforhold_per_sektor,
    arbeidsforhold_norge,
    virksomheter: list[Virksomhet],
    driftsdager,
    stillingsprosent_norge,
):
    ################################################ publiseringsdato.json ################################################

    generer_publiseringsdato(årstall, kvartal, output_dir)

    ################################################ land.json ################################################

    prosent_norge = beregn_sykefraværsprosent(seed=årstall + kvartal, tall=6.0)

    generer_land(
        årstall=årstall,
        kvartal=kvartal,
        prosent_norge=prosent_norge,
        arbeidsforhold=arbeidsforhold_norge,
        stillingsprosent=stillingsprosent_norge,
        driftsdager=driftsdager,
        output_dir=output_dir,
    )

    ################################################ sektor.json ################################################

    sektor_data = []
    sektorer = ["1", "2", "3"]

    for sektor in sektorer:
        sektor_data.append(
            generer_sektor(
                årstall=årstall,
                kvartal=kvartal,
                prosent_norge=prosent_norge,
                sektor=sektor,
                arbeidsforhold=arbeidsforhold_per_sektor[sektor],
                stillingsprosent_norge=stillingsprosent_norge,
                driftsdager=driftsdager,
            )
        )

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/sektor.json"
    skriv_til_fil(data=sektor_data, filnavn=filnavn)

    ################################################ naering.json ################################################

    næring_data = []
    næringer = list(arbeidsforhold_per_næring.keys())
    for næring in næringer:
        næring_data.append(
            generer_næring(
                årstall=årstall,
                kvartal=kvartal,
                prosent_norge=prosent_norge,
                næring=næring,
                arbeidsforhold=arbeidsforhold_per_næring[næring],
                stillingsprosent_norge=stillingsprosent_norge,
                driftsdager=driftsdager,
            )
        )

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/naering.json"
    skriv_til_fil(data=næring_data, filnavn=filnavn)

    ################################################ naeringskode.json ################################################

    næringskode_data = []
    næringskoder = list(arbeidsforhold_per_næringskode.keys())
    for næringskode in næringskoder:
        næringskode_data.append(
            generer_næringskode(
                årstall=årstall,
                kvartal=kvartal,
                prosent_norge=prosent_norge,
                næringskode=næringskode,
                arbeidsforhold=arbeidsforhold_per_næringskode[næringskode],
                stillingsprosent_norge=stillingsprosent_norge,
                driftsdager=driftsdager,
            )
        )

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/naeringskode.json"
    skriv_til_fil(data=næringskode_data, filnavn=filnavn)

    ################################################ Virksomheter ################################################

    virksomhet_json = []
    virksomhet_metadata_json = []

    for virksomhet in virksomheter:
        virksomhet_dict, virksomhet_metadata_dict = generer_virksomhet(
            årstall=årstall,
            kvartal=kvartal,
            prosent_norge=prosent_norge,
            virksomhet=virksomhet,
            stillingsprosent_norge=stillingsprosent_norge,
            driftsdager=driftsdager,
        )
        virksomhet_json.append(virksomhet_dict)
        virksomhet_metadata_json.append(virksomhet_metadata_dict)

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/virksomhet.json"
    skriv_til_fil(data=virksomhet_json, filnavn=filnavn)

    filnavn = f"{output_dir}/{årstall}/K{kvartal}/virksomhet_metadata.json"
    skriv_til_fil(data=virksomhet_metadata_json, filnavn=filnavn)

    logging.debug(f"{årstall}/K{kvartal} ferdig")


def main():
    gjennomsnittlig_stillingsprosent = 0.85
    driftsdager_per_kvartal = {1: 63, 2: 64, 3: 66, 4: 63}

    (
        næringer,
        næringskoder,
        arbeidsforhold_per_næring,
        arbeidsforhold_per_næringskode,
        arbeidsforhold_per_sektor,
        arbeidsforhold_nasjonalt,
    ) = les_fra_filer()

    virksomheter = last_virksomheter()

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
                arbeidsforhold_norge=arbeidsforhold_nasjonalt,
                virksomheter=virksomheter,
                driftsdager=driftsdager_per_kvartal[kvartal],
                stillingsprosent_norge=gjennomsnittlig_stillingsprosent,
            )

            # Gamle virksomheter:

            rectype_1_virksomheter = [
                "947195360",
                "944732004",
                "945071842",
                "945971401",
            ]


if __name__ == "__main__":
    main()

    # TODO: Hvis vi tar utgangspunkt i ekte antall arbeidsforhold per næring, næringskode, sektor
    # så kan vi generere sykefravær basert på dette. Det kan føre til et problem om vi har en virksomhet som det ikke er arbeidsforhold for. f.eks. dyrking av ris
