## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, pruning columns, materializing expensive CTEs, and improving join strategies for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce scan and shuffle
- Broadcast joins and filter pushdown improve join efficiency
- Materializing expensive CTEs simulates caching to avoid recomputation
- Precomputing aggregations and reducing redundant window functions lower computational cost

> This PR was generated automatically. Please run tests and review carefully.
