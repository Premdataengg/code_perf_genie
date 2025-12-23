## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns, reordering joins, simulating caching, and reducing redundant computations, resulting in improved Spark SQL performance without changing output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter and column pruning reduce data scanned and shuffled
- Join order and broadcast hints minimize shuffle and memory usage
- Materializing expensive CTEs simulates caching to avoid recomputation
- Window function and aggregation optimizations reduce computation overhead

> This PR was generated automatically. Please run tests and review carefully.
