## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pruning columns, adding broadcast hints, restructuring joins and filters for predicate pushdown, reducing window/aggregation complexity, and suggesting caching for expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column pruning and predicate pushdown reduce data scanned and shuffled.
- Broadcast hints and join order improve join performance.
- Window and aggregation simplification reduces computation and memory usage.
- Caching intermediate results avoids redundant computation.

> This PR was generated automatically. Please run tests and review carefully.
