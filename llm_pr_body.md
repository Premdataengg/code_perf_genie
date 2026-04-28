## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing filters early, pruning columns, reordering joins, and reducing redundant computations and window functions. CTEs are used for predicate pushdown and to simulate caching.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Early filtering and column pruning reduce data scanned and shuffled
- Join order and broadcast hints improve join efficiency
- Window and aggregation functions reduced to minimize computation
- Subquery aggregations replaced with pre-aggregated CTEs for memory efficiency

> This PR was generated automatically. Please run tests and review carefully.
