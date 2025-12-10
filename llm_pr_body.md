## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying filter pushdown, column pruning, efficient join order, window function reduction, and replacing correlated subqueries with joins/aggregations. These changes improve query performance while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Filter pushdown and column pruning reduce data scanned and shuffled.
- Materializing CTEs simulates caching, reducing recomputation.
- Efficient join order and broadcast hints minimize shuffle and memory usage.
- Precomputing aggregations and replacing correlated subqueries with joins improves execution speed.

> This PR was generated automatically. Please run tests and review carefully.
