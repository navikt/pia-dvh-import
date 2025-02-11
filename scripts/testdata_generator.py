import logging

from les_og_skriv_til_fil import skriv_til_fil
from utregninger import (
    beregn_mulige_dagsverk,
    beregn_sykefraværsprosent,
    beregn_tapte_dagsverk,
    fordel_tapte_dagsverk,
    pluss_minus_prosent,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def generer_publiseringsdato(årstall: int, kvartal: int, output_dir: str):
    logging.debug(f"Genererer publiseringsdato for {årstall}K{kvartal}")

    # TODO: Skriv publiseringsdato til fil
    # publiseringsdato = [
    #     {
    #         "rapport_periode": "202403",
    #         "offentlig_dato": "2024-11-28, 08:00:00",
    #         "oppdatert_i_dvh": "2023-10-20",
    #     },
    #     {
    #         "rapport_periode": "202402",
    #         "offentlig_dato": "2024-09-05, 08:00:00",
    #         "oppdatert_i_dvh": "2023-10-20",
    #     },
    #     {
    #         "rapport_periode": "202401",
    #         "offentlig_dato": "2024-05-30, 08:00:00",
    #         "oppdatert_i_dvh": "2023-10-20",
    #     },
    #     {
    #         "rapport_periode": "202304",
    #         "offentlig_dato": "2024-02-29, 08:00:00",
    #         "oppdatert_i_dvh": "2023-10-20",
    #     },
    # ]

    # filnavn = f"{output_dir}/{årstall}/publiseringsdato.json"
    # skriv_til_fil(data=publiseringsdato, filnavn=filnavn)


def generer_land(
    årstall,
    kvartal,
    prosent_norge,
    arbeidsforhold,
    stillingsprosent,
    driftsdager,
    output_dir,
):
    logging.debug("Genererer data for land")

    seed = årstall + kvartal

    antall_personer = round(pluss_minus_prosent(seed=seed, tall=arbeidsforhold))

    mulige_dagsverk = beregn_mulige_dagsverk(
        antall_personer=antall_personer,
        stillingsprosent=stillingsprosent,
        driftsdager=driftsdager,
    )

    tapte_dagsverk = beregn_tapte_dagsverk(
        mulige_dagsverk=mulige_dagsverk,
        sykefraværsprosent=prosent_norge,
    )

    land_data = [
        {
            "land": "NO",
            "årstall": årstall,
            "kvartal": kvartal,
            "prosent": prosent_norge,
            "tapteDagsverk": tapte_dagsverk,
            "muligeDagsverk": mulige_dagsverk,
            "antallPersoner": antall_personer,
        }
    ]

    skriv_til_fil(
        data=land_data, filnavn=f"{output_dir}/{årstall}/K{kvartal}/land.json"
    )


def generer_sektor(
    årstall,
    kvartal,
    prosent_norge,
    sektor,
    arbeidsforhold,
    stillingsprosent_norge,
    driftsdager,
):
    logging.debug(f"Genererer data for sektor {sektor}")

    seed = årstall + kvartal + int(sektor)

    antall_personer = round(pluss_minus_prosent(seed=seed, tall=arbeidsforhold))

    prosent = beregn_sykefraværsprosent(seed=seed, tall=prosent_norge)

    stillingsprosent = pluss_minus_prosent(
        seed=seed,
        tall=stillingsprosent_norge,
    )

    mulige_dagsverk = beregn_mulige_dagsverk(
        antall_personer=antall_personer,
        stillingsprosent=stillingsprosent,
        driftsdager=driftsdager,
    )

    tapte_dagsverk = beregn_tapte_dagsverk(
        mulige_dagsverk=mulige_dagsverk,
        sykefraværsprosent=prosent,
    )

    return {
        "sektor": sektor,
        "årstall": årstall,
        "kvartal": kvartal,
        "prosent": prosent,
        "tapteDagsverk": tapte_dagsverk,
        "muligeDagsverk": mulige_dagsverk,
        "antallPersoner": antall_personer,
    }


def generer_næring(
    årstall,
    kvartal,
    prosent_norge,
    næring,
    arbeidsforhold,
    stillingsprosent_norge,
    driftsdager,
):
    logging.debug(f"Genererer data for næring: {næring}")
    seed = årstall + kvartal + int(næring)

    prosent = beregn_sykefraværsprosent(seed=seed, tall=prosent_norge)
    antall_personer = round(pluss_minus_prosent(seed=seed, tall=arbeidsforhold))

    stillingsprosent = pluss_minus_prosent(
        seed=seed,
        tall=stillingsprosent_norge,
    )

    mulige_dagsverk = beregn_mulige_dagsverk(
        antall_personer=antall_personer,
        stillingsprosent=stillingsprosent,
        driftsdager=driftsdager,
    )

    tapte_dagsverk = beregn_tapte_dagsverk(
        mulige_dagsverk=mulige_dagsverk,
        sykefraværsprosent=prosent,
    )

    tapte_dagsverk_gradert = round(tapte_dagsverk * stillingsprosent * 0.5, 6)

    tapte_dagsverk_per_varighet = fordel_tapte_dagsverk(
        seed=seed,
        tapte_dagsverk=tapte_dagsverk,
        inkluder_alle_varigheter=True,  # Får alltid A, B, C, D, E, F
    )

    return {
        "næring": næring,
        "årstall": årstall,
        "kvartal": kvartal,
        "prosent": prosent,
        "tapteDagsverk": tapte_dagsverk,
        "muligeDagsverk": mulige_dagsverk,
        "tapteDagsverkGradert": tapte_dagsverk_gradert,
        "tapteDagsverkPerVarighet": [
            {"varighet": key, "tapteDagsverk": tapte_dagsverk_per_varighet[key]}
            for key in tapte_dagsverk_per_varighet
        ],
        "antallPersoner": antall_personer,
    }


def generer_næringskode(
    årstall,
    kvartal,
    prosent_norge,
    næringskode,
    arbeidsforhold,
    stillingsprosent_norge,
    driftsdager,
):
    logging.debug(f"Genererer data for næringskode {næringskode}")
    seed = årstall + kvartal + int(næringskode)

    prosent = beregn_sykefraværsprosent(seed=seed, tall=prosent_norge)
    antall_personer = round(pluss_minus_prosent(seed=seed, tall=arbeidsforhold))

    stillingsprosent = pluss_minus_prosent(
        seed=seed,
        tall=stillingsprosent_norge,
    )

    mulige_dagsverk = beregn_mulige_dagsverk(
        antall_personer=antall_personer,
        stillingsprosent=stillingsprosent,
        driftsdager=driftsdager,
    )

    tapte_dagsverk = beregn_tapte_dagsverk(
        mulige_dagsverk=mulige_dagsverk,
        sykefraværsprosent=prosent,
    )

    tapte_dagsverk_gradert = tapte_dagsverk * stillingsprosent * 0.5

    tapte_dagsverk_per_varighet = fordel_tapte_dagsverk(
        seed=seed,
        tapte_dagsverk=tapte_dagsverk,
        inkluder_alle_varigheter=True,  # Får alltid A, B, C, D, E, F
    )

    return {
        "næringskode": næringskode,
        "årstall": årstall,
        "kvartal": kvartal,
        "prosent": prosent,
        "tapteDagsverk": tapte_dagsverk,
        "muligeDagsverk": mulige_dagsverk,
        "tapteDagsverkGradert": tapte_dagsverk_gradert,
        "tapteDagsverkPerVarighet": [
            {"varighet": key, "tapteDagsverk": tapte_dagsverk_per_varighet[key]}
            for key in tapte_dagsverk_per_varighet
        ],
        "antallPersoner": antall_personer,
    }


def generer_virksomhet(
    årstall,
    kvartal,
    prosent_norge,
    virksomhet,
    stillingsprosent_norge,
    driftsdager,
    rectype="2",
):
    logging.debug(f"Genererer data for virksomhet: {virksomhet.orgnummer}")
    # Sektor virker å være utledet fra: https://www.ssb.no/klass/klassifikasjoner/39/koder som vi kanskje har i virksomheten
    sektor = str(int(virksomhet.orgnummer) % 3 + 1)
    næring = virksomhet.næringskode.split(".")[0]
    næringskode = virksomhet.næringskode.replace(".", "")

    seed_virksomhet = årstall + kvartal + int(virksomhet.orgnummer)

    prosent = beregn_sykefraværsprosent(
        seed=seed_virksomhet, tall=prosent_norge, prosent=0.8
    )

    stillingsprosent = pluss_minus_prosent(
        seed=seed_virksomhet,
        tall=stillingsprosent_norge,
    )

    mulige_dagsverk = beregn_mulige_dagsverk(
        antall_personer=virksomhet.antall_ansatte,
        stillingsprosent=stillingsprosent,
        driftsdager=driftsdager,
    )

    tapte_dagsverk = beregn_tapte_dagsverk(
        mulige_dagsverk=mulige_dagsverk,
        sykefraværsprosent=prosent,
    )

    tapte_dagsverk_gradert = round(tapte_dagsverk * stillingsprosent * 0.5, 6)

    tapte_dagsverk_per_varighet = fordel_tapte_dagsverk(
        seed=seed_virksomhet,
        tapte_dagsverk=tapte_dagsverk,
    )

    virksomhet_json = {
        "orgnr": virksomhet.orgnummer,
        "årstall": årstall,
        "kvartal": kvartal,
        "prosent": prosent,
        "tapteDagsverk": tapte_dagsverk,
        "muligeDagsverk": mulige_dagsverk,
        "tapteDagsverkGradert": tapte_dagsverk_gradert,
        "tapteDagsverkPerVarighet": [
            {"varighet": key, "tapteDagsverk": tapte_dagsverk_per_varighet[key]}
            for key in tapte_dagsverk_per_varighet
        ],
        "antallPersoner": virksomhet.antall_ansatte,
        "rectype": rectype,
    }

    virksomhet_metadata_json = {
        "orgnr": virksomhet.orgnummer,
        "årstall": årstall,
        "kvartal": kvartal,
        "sektor": sektor,
        "primærnæring": næring,
        "primærnæringskode": næringskode,
        "rectype": rectype,
    }

    return virksomhet_json, virksomhet_metadata_json
