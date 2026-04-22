## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join strategies, caching, and aggregation structure. Redundant computations and full scans were reduced for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage.
- Broadcast joins and join order improve shuffle and join performance.
- Caching intermediate CTEs avoids recomputation of expensive aggregations.
- Pre-aggregation and CTEs reduce redundant window and aggregation calculations.

> This PR was generated automatically. Please run tests and review carefully.
