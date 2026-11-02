# Test datasets

Known answers, so results can be checked during the demonstration
(prime counts verified independently with SymPy).

| File | Values | Smallest | Largest (= MAX) | PRIMECOUNT |
|---|---:|---:|---:|---:|
| `numbers_small.csv` | 20 | 3 | 998 | 7 |
| `numbers_100k.csv` | 100,000 | 2,806,348 | 999,991,518,681 | 3,674 |
| `numbers_large_values_5k.csv` | 5,000 | 104,343,376,786,644,333 | 9,223,372,036,854,775,807 (largest 64-bit value) | 103 |

| PRIMESUM range | Expected sum |
|---|---:|
| 1 .. 1,000,000 (`primesum_range.csv`) | 37,550,402,023 |
| 1 .. 1,000,000,000 | 24,739,512,092,254,535 |

Generate a bigger file (e.g. one million values) from the client GUI ("Generate...")
or from the command line:

```
java -cp target\distrilab.jar client.Client --generate --count=1000000 --min=1 --max=1000000000000 --out=data\generated_1m.csv
```
