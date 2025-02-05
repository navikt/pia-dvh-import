import random


class Virksomhet:
    def __init__(
        self,
        orgnummer,
        navn,
        næringskode,
        kommune,
        postnummer,
        adresse_lines,
        kommunenummer,
        poststed,
        landkode,
        land,
        oppstartsdato,
        antall_ansatte,
    ):
        self.orgnummer = orgnummer
        self.navn = navn
        self.næringskode = næringskode
        self.kommune = kommune
        self.postnummer = postnummer
        self.adresse_lines = adresse_lines
        self.kommunenummer = kommunenummer
        self.poststed = poststed
        self.landkode = landkode
        self.land = land
        self.oppstartsdato = oppstartsdato
        self.antall_ansatte = antall_ansatte

        self.sektor = "-1000"  # TODO
        self.rectype = "-1000"  # TODO

    def __eq__(self, other):
        return self.orgnummer == other.orgnummer

    def __hash__(self):
        return hash(("orgnummer", self.orgnummer))

    def til_virksomhet_metadata_json(self, årstall: int, kvartal: int):
        return {
            "orgnr": self.orgnummer,
            "årstall": årstall,
            "kvartal": kvartal,
            "sektor": self.sektor,
            "primærnæring": self.næringskode.split(".")[0],
            "primærnæringskode": self.næringskode.replace(".", ""),
            "rectype": self.rectype,
        }

    def til_virksomhet_json(self, årstall: int, kvartal: int, nasjonalt_sykefravær):
        random.seed(årstall + kvartal + int(self.orgnummer))

        # TODO: Finn et tall å bruke for driftsdager?
        driftsdager = {1: 63, 2: 64, 3: 66, 4: 63}
        gjennomsnittlig_stillingsprosent = 0.85

        sykefraværsprosent = self.sykefraværprosent(
            årstall, kvartal, nasjonalt_sykefravær
        )

        mulige_dagsverk = (
            self.antall_ansatte
            * driftsdager[kvartal]
            * gjennomsnittlig_stillingsprosent
        )

        tapte_dagsverk = mulige_dagsverk * sykefraværsprosent / 100

        # TODO: Finn en bedre måte å regne ut dette på
        # Bare tilfeldig prosent for gradering av sykefravær
        gradering = random.randint(1, 100) / 100
        tapte_dagsverk_gradert = gradering * tapte_dagsverk

        return {
            "orgnr": self.orgnummer,
            "årstall": årstall,
            "kvartal": kvartal,
            # Sykefraværsprosent = (tapte_dagsverk * 100) / mulige_dagsverk
            "prosent": round(sykefraværsprosent, 2),
            "antallPersoner": self.antall_ansatte,
            # antall arbeidsdager med sykefravær justert for stillingsprosent og sykmeldingsgrad
            "tapteDagsverk": round(tapte_dagsverk, 2),
            # antall ansatte med antall driftsdager pr. kvartal og stillingsandel
            "muligeDagsverk": round(mulige_dagsverk, 2),
            "tapteDagsverkGradert": round(tapte_dagsverk_gradert, 2),
            "tapteDagsverkPerVarighet": self.fordel_tapte_dagsverk(
                tapte_dagsverk, årstall, kvartal
            ),
        }

    def fordel_tapte_dagsverk(self, tapte_dagsverk, årstall: int, kvartal: int):
        varigheter = ["A", "B", "C", "D", "E", "F"]
        result = []
        remaining_dagsverk = tapte_dagsverk

        random.seed(årstall + kvartal + int(self.orgnummer))

        for varighet in varigheter:
            if remaining_dagsverk <= 0:
                break
            if random.choice([True, False]):
                continue
            else:
                tapte_dagsverk_for_varighet = round(
                    random.uniform(5, remaining_dagsverk), 2
                )
                remaining_dagsverk -= tapte_dagsverk_for_varighet
            result.append(
                {"varighet": varighet, "tapteDagsverk": tapte_dagsverk_for_varighet}
            )

        # Adjust the last element to ensure the sum is exactly tapte_dagsverk
        if result and remaining_dagsverk > 0:
            for item in reversed(result):
                if item["tapteDagsverk"] is not None:
                    item["tapteDagsverk"] += remaining_dagsverk
                    break

        # Ensure at least one element is returned
        if not result:
            result.append(
                {"varighet": varigheter[0], "tapteDagsverk": round(tapte_dagsverk, 2)}
            )

        return result

    def sykefraværprosent(self, årstall: int, kvartal: int, nasjonalt_sykefravær=6.0):
        random.seed(årstall + kvartal + int(self.orgnummer))
        return random.uniform(0.5, 4.0) * nasjonalt_sykefravær
