## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join order and hints, caching strategy, and aggregation efficiency using CTEs and approximate functions.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Explicit column selection and filter reordering enable partition pruning and reduce data scanned.
- Join order and broadcast hints minimize shuffle and improve join performance.
- Caching recommendations prevent recomputation of expensive intermediate results.
- Window function and aggregation optimizations reduce redundant computation and memory usage.

> This PR was generated automatically. Please run tests and review carefully.
