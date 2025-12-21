## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pruning columns, using CTEs for reuse, applying broadcast hints, reordering joins, and reducing redundant window functions. These changes improve partition pruning, join efficiency, aggregation, and memory usage while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column pruning and early filtering reduce I/O and memory footprint
- Broadcast hints and join reordering improve join performance
- Materializing expensive CTEs simulates caching and avoids recomputation
- Window function and aggregation optimizations reduce computation overhead

> This PR was generated automatically. Please run tests and review carefully.
