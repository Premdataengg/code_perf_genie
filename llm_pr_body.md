## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by enabling partition pruning, predicate pushdown, efficient join order, column pruning, caching, and replacing correlated subqueries with CTEs. These changes reduce I/O, memory usage, and data shuffling for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column selection reduce unnecessary data scans
- Predicate pushdown and join reordering minimize shuffle and improve execution
- Caching and CTEs avoid recomputation and repeated scans
- Replacing correlated subqueries with joins/CTEs yields significant performance gains

> This PR was generated automatically. Please run tests and review carefully.
