## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by adding partition pruning, join reordering, caching hints, column pruning, and CTEs to reduce data movement and recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition and column pruning reduce I/O and memory usage
- Join reordering and broadcast hints improve join efficiency
- Caching intermediate CTEs avoids recomputation
- Window and aggregation optimizations reduce data shuffling

> This PR was generated automatically. Please run tests and review carefully.
