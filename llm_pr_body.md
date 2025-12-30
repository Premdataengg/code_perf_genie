## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pruning columns, pushing down filters, reordering joins, reducing redundant computations, and replacing correlated subqueries with joins or window functions. These changes improve partition pruning, reduce data shuffling, and lower memory usage.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column pruning and filter pushdown reduce I/O and memory footprint
- Join order and broadcast hints minimize shuffle and improve join efficiency
- Window and aggregation optimizations reduce redundant computation
- Correlated subqueries replaced with joins/window functions for better performance

> This PR was generated automatically. Please run tests and review carefully.
