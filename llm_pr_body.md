## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns, reordering joins, using CTEs for expensive computations, and simulating caching. These changes reduce data shuffling, memory usage, and computation while preserving identical results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column and partition pruning reduces I/O and memory footprint
- Join order and broadcast hints minimize shuffle and improve join efficiency
- CTEs simulate caching and avoid recomputation of expensive aggregations
- Window and aggregation optimizations reduce redundant calculations

> This PR was generated automatically. Please run tests and review carefully.
