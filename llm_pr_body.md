## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, adding partition pruning, using CTEs for predicate pushdown, adding caching hints, and reducing redundant/repeated computations. These changes improve join, aggregation, and memory efficiency while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Broadcast and join order optimizations minimize shuffle and improve join speed
- Caching and CTEs avoid recomputation of expensive intermediate results
- Aggregation and window function simplification reduces computation overhead

> This PR was generated automatically. Please run tests and review carefully.
