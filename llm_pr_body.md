## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by introducing partition pruning, column pruning, broadcast join hints, predicate pushdown, and CTEs for reusable aggregations. These changes reduce data scanned, improve join efficiency, and minimize redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Broadcast joins and predicate pushdown improve join and filter performance
- CTEs and caching hints avoid recomputation of expensive aggregations

> This PR was generated automatically. Please run tests and review carefully.
