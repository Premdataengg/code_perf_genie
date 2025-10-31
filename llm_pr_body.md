## Daily Spark Optimization

**Summary**
Major anti-patterns were optimized by applying partition pruning, predicate pushdown, join reordering, column pruning, and materializing expensive subqueries/aggregations. Window and string operations were reduced or precomputed where possible.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and shuffle.
- Predicate pushdown and join reordering minimize data scanned and shuffled.
- Materializing expensive CTEs simulates caching and avoids recomputation.
- Redundant window and aggregation functions are minimized for efficiency.

> This PR was generated automatically. Please run tests and review carefully.
