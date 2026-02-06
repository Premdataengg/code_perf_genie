## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, selective column projection, join order improvements, broadcast hints only for small tables, and CTE reuse to mimic caching.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column selection reduce data scanned and shuffled.
- Join order and predicate pushdown minimize intermediate data size.
- CTE reuse avoids recomputation of expensive aggregations, improving performance.

> This PR was generated automatically. Please run tests and review carefully.
