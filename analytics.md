Setting default log level to "WARN".

To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

+----+-----+---+---------+

|year|month|day|anomalies|

+----+-----+---+---------+

|2026|    5| 15|     NULL|

+----+-----+---+---------+



+-----------+------------------+---------+---------+-----------------+----------------+

|sensor\_type|        mean\_value|min\_value|max\_value|     stddev\_value|anomaly\_rate\_pct|

+-----------+------------------+---------+---------+-----------------+----------------+

|   humidity| 25.71837837837837|    10.71|     39.4|9.257684949847379|            NULL|

|temperature|24.817500000000003|    10.78|    36.81|8.040851343146487|            NULL|

|   pressure| 26.02032258064516|    11.23|    38.94|7.759496755104233|            NULL|

+-----------+------------------+---------+---------+-----------------+----------------+



+----+-----+---+------------------+-------------+

|year|month|day|          avg\_temp|anomaly\_count|

+----+-----+---+------------------+-------------+

|2026|    5| 15|24.817500000000003|         NULL|

+----+-----+---+------------------+-------------+





=== Partition Pruning Demo ===

Full scan: 100 records, 0.16s

Filtered scan: 32 records, 0.11s

Speedup: 1.4x

