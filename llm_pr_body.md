## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, column pruning, join reordering, filter pushdown, caching hints, and reducing redundant window functions for improved Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Join reordering and filter pushdown minimize data shuffled and processed
- Caching hints prevent recomputation of expensive intermediate results
- Reducing redundant window functions and aggregations lowers computation cost

> This PR was generated automatically. Please run tests and review carefully.
