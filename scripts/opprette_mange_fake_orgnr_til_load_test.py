import json

with open("output/virksomhet.json", "w") as virksomhet_fil:
    virksomhet_fil.write("[\n")
    counter = 1
    range_from = 300000000
    nb_of_rows = 5000  # veldig mange: 370000
    range_to = range_from + nb_of_rows
    for orgnr in range(range_from, range_to):
        virksomhet_statistikk = {
            "årstall": 2024,
            "kvartal": 1,
            "orgnr": f"{orgnr}",
            "prosent": 8.2,
            "tapteDagsverk": 454.997303,
            "muligeDagsverk": 5580.740995,
            "antallPersoner": 128,
            "rectype": "2",
            "tapteDagsverkGradert": 136.897803,
            "tapteDagsverkPerVarighet": [
                {"varighet": "A", "tapteDagsverk": None},
                {"varighet": "B", "tapteDagsverk": 11.1743},
                {"varighet": "C", "tapteDagsverk": 58.8988},
                {"varighet": "D", "tapteDagsverk": 91},
                {"varighet": "E", "tapteDagsverk": 206.694203},
                {"varighet": "F", "tapteDagsverk": 62},
            ],
        }
        counter = counter + 1
        row = json.dumps(virksomhet_statistikk, ensure_ascii=False)
        if counter <= nb_of_rows:
            virksomhet_fil.write(f"{row},\n")
        else:
            virksomhet_fil.write(f"{row}\n")
    virksomhet_fil.write("]\n")
print(f"----- {counter}")
virksomhet_fil.close()
