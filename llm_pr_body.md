## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pruning columns, reordering joins, adding caching, reducing redundant window functions, and pushing down filters for partition and predicate pruning.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and improve partition pruning.
- Join reordering and broadcast hints optimize join strategies for large and small tables.
- Caching and window function reduction minimize recomputation and memory usage.
- Replacing subqueries with window functions and CTEs improves aggregation efficiency.

> This PR was generated automatically. Please run tests and review carefully.
