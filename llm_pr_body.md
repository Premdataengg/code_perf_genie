## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, column selection, join reordering, CTE reuse, and aggregation precomputation to reduce data scanned and improve execution efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage
- Join reordering and predicate pushdown improve join performance
- CTE reuse simulates caching, reducing recomputation of expensive aggregations
- Precomputing and reusing aggregations minimizes redundant window function computation

> This PR was generated automatically. Please run tests and review carefully.
