## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying partition pruning, column pruning, join order optimization, caching, and reducing redundant window functions for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Explicit partition pruning and column selection reduce data scanned and shuffled.
- Join order and broadcast hints now only applied to small tables, with filters pushed before joins.
- Caching added to expensive CTEs to avoid recomputation.
- Redundant window functions reduced and approximate aggregations used for large datasets.

> This PR was generated automatically. Please run tests and review carefully.
