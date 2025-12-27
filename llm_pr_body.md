## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by introducing column pruning, partition pruning, predicate pushdown, CTE caching hints, join reordering, and aggregation reuse to reduce data movement and computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and partition pruning reduce data scanned and shuffled.
- Predicate pushdown and join reordering improve query execution plans.
- Aggregation reuse and window function reduction minimize recomputation.
- Caching hints (as comments) encourage Spark to avoid recomputation of expensive CTEs.

> This PR was generated automatically. Please run tests and review carefully.
