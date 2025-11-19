## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, predicate pushdown, join reordering, simulated caching, column pruning, and reducing redundant window functions.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce data scanned and improve query speed.
- Join reordering and broadcast hints optimize join execution plans.
- Materializing expensive CTEs simulates caching, reducing recomputation.
- Reducing redundant window functions and precomputing aggregations minimizes multiple data passes.

> This PR was generated automatically. Please run tests and review carefully.
