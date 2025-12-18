## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by improving partition pruning, join order, predicate pushdown, column pruning, and window/aggregation efficiency. Added hints and structural changes to reduce data scanned and computation cost.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Join order and predicate pushdown improve join performance and reduce shuffle
- Window and aggregation optimizations minimize data passes and computation

> This PR was generated automatically. Please run tests and review carefully.
