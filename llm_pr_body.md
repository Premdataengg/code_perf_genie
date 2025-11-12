## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying column pruning, filter pushdown, join order improvements, CTE reuse for caching, and precomputing aggregations to reduce redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and improve partition pruning.
- Join order and broadcast hints minimize shuffle and memory usage.
- CTE reuse simulates caching, reducing repeated expensive computation.
- Precomputing aggregations and reusing results reduces window function overhead.

> This PR was generated automatically. Please run tests and review carefully.
