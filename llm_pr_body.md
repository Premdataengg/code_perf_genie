## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by adding partition pruning, column pruning, broadcast hints, predicate pushdown, CTE reuse, and caching strategies. These changes reduce I/O, memory usage, and shuffle, while preserving identical query results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce scan and memory footprint
- Broadcast hints and join reordering minimize shuffle and improve join performance
- Caching and CTE reuse avoid redundant computation and repeated aggregations

> This PR was generated automatically. Please run tests and review carefully.
