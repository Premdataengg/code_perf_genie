## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by applying column pruning, partition pruning, join reordering, predicate pushdown, caching hints, and reducing redundant window/aggregation functions.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and partition pruning reduce I/O and memory usage
- Join reordering and predicate pushdown improve join efficiency
- Caching intermediate CTEs avoids recomputation
- Reduced redundant window functions and used APPROX_COUNT_DISTINCT for scalability

> This PR was generated automatically. Please run tests and review carefully.
