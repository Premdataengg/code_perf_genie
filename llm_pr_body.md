## Daily Spark Optimization

**Summary**
Optimized all anti-pattern SQL files by pushing down filters, using CTEs for predicate pushdown and partition pruning, reducing window/aggregation complexity, and selecting only necessary columns.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter application enables partition pruning and reduces data scanned
- CTEs and column pruning reduce memory and computation overhead
- Broadcast hints are used only for small tables
- Window and aggregation functions are minimized or reused for efficiency

> This PR was generated automatically. Please run tests and review carefully.
