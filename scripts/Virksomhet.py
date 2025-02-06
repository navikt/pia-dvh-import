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

    def __eq__(self, other):
        return self.orgnummer == other.orgnummer

    def __hash__(self):
        return hash(("orgnummer", self.orgnummer))

    def til_virksomhet_json(self, årstall: int, kvartal: int, nasjonalt_sykefravær):
        random.seed(årstall + kvartal + int(self.orgnummer))

        # TODO: Finn et tall å bruke for driftsdager?
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

