## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by introducing partition pruning, predicate pushdown, join reordering, materialized CTEs to simulate caching, column pruning, and precomputed aggregations to reduce redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled
- Join reordering and predicate pushdown minimize intermediate data size and improve join efficiency
- Materialized CTEs simulate caching, reducing recomputation of expensive aggregations
- Precomputing aggregations and pruning redundant window functions significantly improve aggregation performance

> This PR was generated automatically. Please run tests and review carefully.
