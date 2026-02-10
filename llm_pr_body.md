## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying column pruning, predicate pushdown, join reordering, CTE reuse, and reducing redundant window functions for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and improve partition pruning.
- Join reordering and selective broadcast minimize shuffle and improve join efficiency.
- CTE reuse simulates caching and avoids recomputation of expensive aggregations.
- Reducing redundant window functions and aggregations lowers computation and memory usage.

> This PR was generated automatically. Please run tests and review carefully.
