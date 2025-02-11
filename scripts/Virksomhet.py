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
