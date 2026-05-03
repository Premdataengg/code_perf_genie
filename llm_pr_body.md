## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, pruning columns, using CTEs for reuse, adding broadcast hints, and replacing subquery aggregations with joins. This reduces data shuffling, memory usage, and redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and predicate pushdown reduce full scans and shuffle.
- Column pruning and CTEs avoid unnecessary data movement and recomputation.
- Broadcast hints and join reordering improve join efficiency.
- Replacing subquery aggregations with joins reduces repeated scans.

> This PR was generated automatically. Please run tests and review carefully.
