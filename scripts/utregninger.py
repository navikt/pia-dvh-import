import random


def fordel_tapte_dagsverk(
    seed,
    tapte_dagsverk,
    inkluder_alle_varigheter=False,
):
    random.seed(seed)
    varigheter = ["A", "B", "C", "D", "E", "F"]

    if not inkluder_alle_varigheter:
        # drop tilfeldig antall elementer
        for _ in range(random.randint(1, len(varigheter) - 1)):
            # drop tilfeldig element fra varigheter
            varigheter.pop(random.randint(0, len(varigheter) - 1))

    distribution = {varighet: 0.0 for varighet in varigheter}

    # Distribute the tapte_dagsverk randomly
    remaining_dagsverk = tapte_dagsverk
    for i in range(len(varigheter) - 1):
        # Randomly decide the amount to distribute to the current varighet
        amount = round(random.uniform(0, remaining_dagsverk), 2)
        distribution[varigheter[i]] += amount
        remaining_dagsverk -= amount

    # Assign the remaining amount to the last varighet
    distribution[varigheter[-1]] += round(remaining_dagsverk, 2)

    return distribution


def beregn_sykefraværsprosent(seed, tall, prosent=0.1):
    return round(pluss_minus_prosent(seed=seed, tall=tall, prosent=prosent), 1)


def pluss_minus_prosent(seed: int, tall: float, prosent: float = 0.1) -> float:
    random.seed(seed)
    return tall * random.uniform(1 - prosent, 1 + prosent)  # +/- 10%


def beregn_mulige_dagsverk(
    antall_personer: int, stillingsprosent: float, driftsdager: int
) -> float:
    return round(antall_personer * stillingsprosent * driftsdager, 6)


def beregn_tapte_dagsverk(mulige_dagsverk: float, sykefraværsprosent: float) -> float:
    return round(mulige_dagsverk * sykefraværsprosent / 100, 6)
