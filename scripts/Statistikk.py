import random
from dataclasses import dataclass

from Virksomhet import Virksomhet


def fordel_tapte_dagsverk(
    seed: int,
    tapte_dagsverk: float,
    inkluder_alle_varigheter: bool = False,
) -> dict[str, float]:
    # TODO: Rydd opp i denne funksjonen
    random.seed(seed)
    varigheter: list[str] = ["A", "B", "C", "D", "E", "F"]

    if not inkluder_alle_varigheter:
        # drop tilfeldig antall elementer
        for _ in range(random.randint(1, len(varigheter) - 1)):
            # drop tilfeldig element fra varigheter
            varigheter.pop(random.randint(0, len(varigheter) - 1))

    distribution: dict[str, float] = {varighet: 0.0 for varighet in varigheter}

    # Distribute the tapte_dagsverk randomly
    remaining_dagsverk: float = tapte_dagsverk
    for i in range(len(varigheter) - 1):
        # Randomly decide the amount to distribute to the current varighet
        amount: float = round(random.uniform(0, remaining_dagsverk), 2)
        distribution[varigheter[i]] += amount
        remaining_dagsverk -= amount

    # Assign the remaining amount to the last varighet
    distribution[varigheter[-1]] += round(remaining_dagsverk, 2)

    return distribution


def varier_med_prosent(
    seed: int,
    tall: float,
    prosent: float = 0.1,
    antall_desimaler: int | None = None,
) -> float:
    random.seed(seed)
    result: float = tall * random.uniform(1 - prosent, 1 + prosent)

    if antall_desimaler is not None:
        result = round(result, antall_desimaler)

    return result


def beregn_mulige_dagsverk(
    antall_personer: int,
    stillingsprosent: float,
    driftsdager: int,
) -> float:
    return round(antall_personer * stillingsprosent * driftsdager, 6)


def beregn_tapte_dagsverk(
    mulige_dagsverk: float,
    sykefraværsprosent: float,
) -> float:
    return round(mulige_dagsverk * sykefraværsprosent / 100, 6)


@dataclass(slots=True)
class TapteDagsverkVarighet:
    varighet: str
    tapteDagsverk: float


@dataclass(slots=True, init=False)
class LandStatistikk:
    land: str
    årstall: int
    kvartal: int
    prosent: float
    tapteDagsverk: float
    muligeDagsverk: float
    antallPersoner: int

    def __init__(
        self,
        årstall: int,
        kvartal: int,
        prosent_norge: float,
        arbeidsforhold_norge: int,
        stillingsprosent_norge: float,
        driftsdager: int,
    ) -> None:
        seed: int = årstall + kvartal
        antall_personer: int = round(
            varier_med_prosent(seed=seed, tall=arbeidsforhold_norge)
        )

        mulige_dagsverk: float = beregn_mulige_dagsverk(
            antall_personer=antall_personer,
            stillingsprosent=stillingsprosent_norge,
            driftsdager=driftsdager,
        )

        tapte_dagsverk: float = beregn_tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk,
            sykefraværsprosent=prosent_norge,
        )

        self.land = "NO"
        self.årstall = årstall
        self.kvartal = kvartal
        self.prosent = prosent_norge
        self.tapteDagsverk = tapte_dagsverk
        self.muligeDagsverk = mulige_dagsverk
        self.antallPersoner = antall_personer


@dataclass(slots=True, init=False)
class SektorStatistikk:
    sektor: str
    årstall: int
    kvartal: int
    prosent: float
    tapteDagsverk: float
    muligeDagsverk: float
    antallPersoner: int

    def __init__(
        self,
        årstall: int,
        kvartal: int,
        prosent_norge: float,
        sektor: str,
        arbeidsforhold: int,
        stillingsprosent_norge: float,
        driftsdager: int,
    ) -> None:
        seed: int = årstall + kvartal + int(sektor)
        antall_personer: int = round(varier_med_prosent(seed=seed, tall=arbeidsforhold))
        prosent: float = varier_med_prosent(
            seed=seed, tall=prosent_norge, antall_desimaler=1
        )
        stillingsprosent: float = varier_med_prosent(
            seed=seed,
            tall=stillingsprosent_norge,
        )
        mulige_dagsverk: float = beregn_mulige_dagsverk(
            antall_personer=antall_personer,
            stillingsprosent=stillingsprosent,
            driftsdager=driftsdager,
        )
        tapte_dagsverk: float = beregn_tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk,
            sykefraværsprosent=prosent,
        )

        self.sektor = sektor
        self.årstall = årstall
        self.kvartal = kvartal
        self.prosent = prosent
        self.tapteDagsverk = tapte_dagsverk
        self.muligeDagsverk = mulige_dagsverk
        self.antallPersoner = antall_personer


@dataclass(slots=True, init=False)
class NæringStatistikk:
    næring: str
    årstall: int
    kvartal: int
    prosent: float
    tapteDagsverk: float
    muligeDagsverk: float
    tapteDagsverkGradert: float
    tapteDagsverkPerVarighet: list[TapteDagsverkVarighet]
    antallPersoner: int

    def __init__(
        self,
        årstall: int,
        kvartal: int,
        prosent_norge: float,
        næring: str,  # f.eks. "01"
        arbeidsforhold: int,
        stillingsprosent_norge: float,
        driftsdager: int,
    ) -> None:
        seed: int = årstall + kvartal + int(næring)
        prosent: float = varier_med_prosent(
            seed=seed, tall=prosent_norge, antall_desimaler=1
        )

        antall_personer: int = round(varier_med_prosent(seed=seed, tall=arbeidsforhold))
        stillingsprosent: float = varier_med_prosent(
            seed=seed,
            tall=stillingsprosent_norge,
        )
        mulige_dagsverk: float = beregn_mulige_dagsverk(
            antall_personer=antall_personer,
            stillingsprosent=stillingsprosent,
            driftsdager=driftsdager,
        )
        tapte_dagsverk: float = beregn_tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk,
            sykefraværsprosent=prosent,
        )
        tapte_dagsverk_gradert: float = round(
            tapte_dagsverk * stillingsprosent * 0.5, 6
        )

        # TODO: se over denne
        tapte_dagsverk_per_varighet: dict[str, float] = fordel_tapte_dagsverk(
            seed=seed,
            tapte_dagsverk=tapte_dagsverk,
            inkluder_alle_varigheter=True,  # Får alltid A, B, C, D, E, F
        )

        self.næring = næring
        self.årstall = årstall
        self.kvartal = kvartal
        self.prosent = prosent
        self.tapteDagsverk = tapte_dagsverk
        self.muligeDagsverk = mulige_dagsverk
        self.tapteDagsverkGradert = tapte_dagsverk_gradert
        self.tapteDagsverkPerVarighet = [
            # TODO: se over denne
            TapteDagsverkVarighet(
                varighet=varighet, tapteDagsverk=tapte_dagsverk_per_varighet[varighet]
            )
            for varighet in tapte_dagsverk_per_varighet
        ]
        self.antallPersoner = antall_personer


@dataclass(slots=True, init=False)
class NæringskodeStatistikk:
    næringskode: str
    årstall: int
    kvartal: int
    prosent: float
    tapteDagsverk: float
    muligeDagsverk: float
    tapteDagsverkGradert: float
    tapteDagsverkPerVarighet: list[TapteDagsverkVarighet]
    antallPersoner: int

    def __init__(
        self,
        årstall: int,
        kvartal: int,
        prosent_norge: float,
        næringskode: str,
        arbeidsforhold: int,
        stillingsprosent_norge: float,
        driftsdager: int,
    ) -> None:
        seed: int = årstall + kvartal + int(næringskode)
        prosent: float = varier_med_prosent(
            seed=seed, tall=prosent_norge, antall_desimaler=1
        )
        antall_personer: int = round(varier_med_prosent(seed=seed, tall=arbeidsforhold))
        stillingsprosent: float = varier_med_prosent(
            seed=seed,
            tall=stillingsprosent_norge,
        )
        mulige_dagsverk: float = beregn_mulige_dagsverk(
            antall_personer=antall_personer,
            stillingsprosent=stillingsprosent,
            driftsdager=driftsdager,
        )
        tapte_dagsverk: float = beregn_tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk,
            sykefraværsprosent=prosent,
        )
        tapte_dagsverk_gradert: float = round(
            tapte_dagsverk * stillingsprosent * 0.5, 6
        )
        # TODO: Se over denne
        tapte_dagsverk_per_varighet: dict[str, float] = fordel_tapte_dagsverk(
            seed=seed,
            tapte_dagsverk=tapte_dagsverk,
            inkluder_alle_varigheter=True,  # Får alltid A, B, C, D, E, F
        )

        self.næringskode = næringskode
        self.årstall = årstall
        self.kvartal = kvartal
        self.prosent = prosent
        self.tapteDagsverk = tapte_dagsverk
        self.muligeDagsverk = mulige_dagsverk
        self.tapteDagsverkGradert = tapte_dagsverk_gradert
        self.tapteDagsverkPerVarighet = [
            # TODO: se over denne
            TapteDagsverkVarighet(
                varighet=varighet, tapteDagsverk=tapte_dagsverk_per_varighet[varighet]
            )
            for varighet in tapte_dagsverk_per_varighet
        ]
        self.antallPersoner = antall_personer


@dataclass(slots=True, init=False)
class VirksomhetStatistikk:
    orgnr: str
    årstall: int
    kvartal: int
    prosent: float
    tapteDagsverk: float
    muligeDagsverk: float
    tapteDagsverkGradert: float
    tapteDagsverkPerVarighet: list[TapteDagsverkVarighet]
    antallPersoner: int
    rectype: str

    def __init__(
        self,
        årstall: int,
        kvartal: int,
        prosent_norge: float,
        virksomhet: Virksomhet,
        stillingsprosent_norge: float,
        driftsdager: int,
    ) -> None:
        seed: int = årstall + kvartal + int(virksomhet.orgnummer)

        prosent: float = varier_med_prosent(
            seed=seed, tall=prosent_norge, prosent=0.8, antall_desimaler=1
        )

        stillingsprosent: float = varier_med_prosent(
            seed=seed,
            tall=stillingsprosent_norge,
        )

        mulige_dagsverk: float = beregn_mulige_dagsverk(
            antall_personer=virksomhet.antall_ansatte,
            stillingsprosent=stillingsprosent,
            driftsdager=driftsdager,
        )
        tapte_dagsverk: float = beregn_tapte_dagsverk(
            mulige_dagsverk=mulige_dagsverk,
            sykefraværsprosent=prosent,
        )
        tapte_dagsverk_gradert: float = round(
            tapte_dagsverk * stillingsprosent * 0.5, 6
        )
        tapte_dagsverk_per_varighet: dict[str, float] = fordel_tapte_dagsverk(
            seed=seed,
            tapte_dagsverk=tapte_dagsverk,
        )

        self.orgnr = virksomhet.orgnummer
        self.årstall = årstall
        self.kvartal = kvartal
        self.prosent = prosent
        self.tapteDagsverk = tapte_dagsverk
        self.muligeDagsverk = mulige_dagsverk
        self.tapteDagsverkGradert = tapte_dagsverk_gradert
        self.tapteDagsverkPerVarighet = [
            TapteDagsverkVarighet(
                varighet=varighet, tapteDagsverk=tapte_dagsverk_per_varighet[varighet]
            )
            for varighet in tapte_dagsverk_per_varighet
        ]
        self.antallPersoner = virksomhet.antall_ansatte
        self.rectype = "2"


@dataclass(slots=True, init=False)
class VirksomhetMetaDataStatistikk:
    orgnr: str
    årstall: int
    kvartal: int
    sektor: str
    primærnæring: str
    primærnæringskode: str
    rectype: str

    def __init__(
        self,
        årstall: int,
        kvartal: int,
        virksomhet: Virksomhet,
    ) -> None:
        sektor = str(int(virksomhet.orgnummer) % 3 + 1)
        næring: str = virksomhet.næringskoder[0].split(".")[0]
        næringskode: str = virksomhet.næringskoder[0].replace(".", "")

        self.orgnr = virksomhet.orgnummer
        self.årstall = årstall
        self.kvartal = kvartal
        self.sektor = sektor
        self.primærnæring = næring
        self.primærnæringskode = næringskode
        self.rectype = "2"
