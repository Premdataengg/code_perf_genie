## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, column pruning, join reordering, predicate pushdown, and reducing redundant aggregations and window functions.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce scanned data and improve IO.
- Join reordering and predicate pushdown minimize shuffle and intermediate data size.
- Precomputing aggregations and using CTEs reduces redundant computations and data passes.

> This PR was generated automatically. Please run tests and review carefully.
