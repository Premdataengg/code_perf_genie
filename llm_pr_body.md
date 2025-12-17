## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, pruning columns, materializing expensive CTEs, and reducing redundant window/aggregation computations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage.
- Join reordering and broadcast hints minimize shuffle and improve join efficiency.
- Materializing expensive aggregations simulates caching and avoids recomputation.
- Precomputing aggregations and minimizing window functions reduce data shuffling and CPU cost.

> This PR was generated automatically. Please run tests and review carefully.
