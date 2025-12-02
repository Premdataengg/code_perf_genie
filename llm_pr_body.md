## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, pruning columns, using CTEs for reuse, and reducing redundant computations and window functions.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Join order and broadcast hints minimize shuffle and memory usage.
- CTEs and filter pushdown avoid recomputation and full scans.
- Redundant window and aggregation functions removed for efficiency.

> This PR was generated automatically. Please run tests and review carefully.
