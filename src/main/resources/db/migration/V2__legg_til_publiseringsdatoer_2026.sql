INSERT INTO publiseringsdato (arstall, kvartal, dato, prosessert)
VALUES
    (2025, 4, '2026-02-26', true),
    (2026, 1, '2026-05-28', false),
    (2026, 2, '2026-09-03', false),
    (2026, 3, '2026-11-26', false)
ON CONFLICT (arstall, kvartal) DO NOTHING;
