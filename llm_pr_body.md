## Daily Spark Optimization

**Summary**
Optimized all anti-pattern SQL files by applying partition pruning, join reordering, predicate pushdown, column pruning, CTE materialization, and reducing redundant computations for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Join reordering and predicate pushdown minimize shuffle and computation
- Materializing expensive CTEs simulates caching for reuse
- Pre-aggregating and reducing window functions lowers compute cost

> This PR was generated automatically. Please run tests and review carefully.
