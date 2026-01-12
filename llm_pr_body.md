## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by enabling partition pruning, predicate pushdown, column pruning, join reordering, CTEs, and replacing correlated subqueries with joins. These changes reduce I/O, memory usage, and computation while preserving identical results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory footprint
- Join reordering and predicate pushdown improve join efficiency
- CTEs and replacing correlated subqueries reduce redundant computation
- Encouraged caching and efficient aggregation strategies

> This PR was generated automatically. Please run tests and review carefully.
