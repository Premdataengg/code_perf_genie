## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by adding partition pruning, predicate pushdown, CTE reuse, efficient join order, and column pruning. Reduced redundant computations and improved window/aggregation efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column selection reduce data scanned and shuffled.
- Predicate pushdown and CTE reuse minimize repeated computation and improve join efficiency.
- Window and aggregation optimizations reduce resource usage and speed up queries.

> This PR was generated automatically. Please run tests and review carefully.
