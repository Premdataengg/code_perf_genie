## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pruning columns, pushing down filters, improving join order, using CTEs for aggregation reuse, and enhancing window function efficiency. These changes reduce I/O, memory, and computation while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Column pruning and predicate pushdown reduce scan size and memory usage
- Join order and broadcast hints improve join performance
- CTEs and aggregation reuse avoid redundant computation
- Window function optimizations reduce shuffle and computation cost

> This PR was generated automatically. Please run tests and review carefully.
