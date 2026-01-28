## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, column pruning, predicate pushdown, join order optimization, and materializing expensive CTEs to mimic caching. Reduced redundant computations and improved query structure for Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Join order and broadcast hints minimize shuffle and memory usage.
- Materializing CTEs simulates caching to avoid recomputation.
- Precomputing aggregations and pruning columns reduces window function overhead.

> This PR was generated automatically. Please run tests and review carefully.
