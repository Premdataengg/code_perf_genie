## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying predicate pushdown, partition pruning, efficient join order, column pruning, and reducing redundant window/aggregation computations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filtering and CTEs reduce data scanned and shuffled.
- Broadcast hints used only for small tables, improving join performance.
- Redundant window and aggregation computations minimized for faster execution.

> This PR was generated automatically. Please run tests and review carefully.
