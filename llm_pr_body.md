## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying partition pruning, predicate pushdown, CTE materialization (simulated caching), join/filter reordering, and window/aggregation reduction for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Join/filter reordering and CTE materialization avoid recomputation and improve query plan efficiency.
- Redundant window and aggregation functions are minimized for faster execution.

> This PR was generated automatically. Please run tests and review carefully.
