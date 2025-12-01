## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partitioning, join order, filter pushdown, column pruning, and reducing redundant or expensive aggregations. Materialized expensive CTEs to simulate caching and used approximate aggregations for large datasets.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and shuffled.
- Join order and early filtering minimize join size and improve broadcast efficiency.
- Materializing CTEs simulates caching to avoid recomputation.
- Approximate aggregations and reduced window functions lower computation cost.

> This PR was generated automatically. Please run tests and review carefully.
