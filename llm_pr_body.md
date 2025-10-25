## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by introducing partition pruning, predicate pushdown, join reordering, column pruning, and simulated caching/materialization of expensive CTEs. Reduced redundant window functions and reused aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and predicate pushdown minimize data scanned and shuffled
- Join reordering and selective broadcast reduce shuffle and improve join efficiency
- Materializing expensive CTEs simulates caching, reducing recomputation
- Column pruning and aggregation reuse minimize data movement and computation

> This PR was generated automatically. Please run tests and review carefully.
