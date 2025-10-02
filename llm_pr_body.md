## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pruning columns, filtering early, adding caching, improving join strategies, and reducing redundant computations for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filtering and column pruning reduce data shuffling and memory usage
- Added caching to avoid recomputation of expensive intermediate results
- Optimized join order and use of broadcast hints for efficient joins
- Precomputed aggregates and reused them to minimize window function overhead

> This PR was generated automatically. Please run tests and review carefully.
