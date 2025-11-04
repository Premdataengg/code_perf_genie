## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, predicate pushdown, join reordering, column pruning, and reducing redundant window functions. CTEs are used for intermediate results to avoid recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition and predicate pushdown reduce data scanned and shuffled.
- Join reordering and broadcast hints minimize shuffle and memory usage.
- Column pruning and limiting output reduce I/O and memory footprint.
- Redundant window functions and aggregations removed for faster execution.

> This PR was generated automatically. Please run tests and review carefully.
