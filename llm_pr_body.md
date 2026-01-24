## Daily Spark Optimization

**Summary**
Major anti-patterns were addressed: partition pruning and column pruning, join reordering and predicate pushdown, simulated caching, window/aggregation reduction, and materializing filtered data to avoid redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage
- Join reordering and predicate pushdown minimize shuffle and data movement
- Materializing CTEs simulates caching and avoids recomputation
- Redundant window and aggregation functions are reduced for efficiency
- Broadcast hints are only used for small tables

> This PR was generated automatically. Please run tests and review carefully.
