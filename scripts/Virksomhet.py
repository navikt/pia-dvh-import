import json
from dataclasses import dataclass


@dataclass(slots=True, eq=False)
class Virksomhet:
    orgnummer: str
    navn: str
    næringskoder: list[str]
    kommune: str
    postnummer: str
    adresse_lines: list[str]
    kommunenummer: str
    poststed: str
    landkode: str
    land: str
    oppstartsdato: str
    antall_ansatte: int

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Virksomhet):
            return NotImplemented
        return self.orgnummer == other.orgnummer

    def __hash__(self) -> int:
        return hash(self.orgnummer)


def les_virksomheter_fra_fil(
    filnavn: str,
) -> list[Virksomhet]:
    with open(filnavn, "r") as tenorTestDataFil:
        json_decode = json.load(tenorTestDataFil)
        return [
            Virksomhet(
                orgnummer=virksomhet.get("organisasjonsnummer"),
                navn=virksomhet.get("navn"),
                næringskoder=virksomhet.get("naeringKode"),
                kommune=virksomhet.get("forretningsadresse").get("kommune"),
                postnummer=virksomhet.get("forretningsadresse").get("postnummer"),
                adresse_lines=[
                    adresse.replace("'", "")
                    for adresse in virksomhet.get("forretningsadresse").get("adresse")
                ],
                kommunenummer=virksomhet.get("forretningsadresse").get("kommunenummer"),
                poststed=virksomhet.get("forretningsadresse").get("poststed"),
                landkode=virksomhet.get("forretningsadresse").get("landkode"),
                land=virksomhet.get("forretningsadresse").get("land"),
                oppstartsdato=virksomhet.get("registreringsdatoEnhetsregisteret"),
                antall_ansatte=virksomhet.get("antallAnsatte"),
            )
            for virksomhet in json_decode["dokumentListe"]
        ]
