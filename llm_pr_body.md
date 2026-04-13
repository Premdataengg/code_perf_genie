## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing filters early, enabling partition pruning, using CTEs for predicate pushdown, and materializing/caching expensive intermediate results. Reduced data shuffling and improved join/aggregation efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Early filtering and column pruning reduce I/O and memory usage
- Materialized/cached CTEs avoid recomputation and improve performance
- Predicate pushdown and partition pruning minimize full table scans
- Join order and broadcast hints are now optimal for query plans

> This PR was generated automatically. Please run tests and review carefully.
