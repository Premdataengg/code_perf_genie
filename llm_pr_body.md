## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by introducing partition pruning, predicate pushdown, efficient join order, caching, column pruning, and reducing redundant window aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Predicate pushdown and join order minimize intermediate data size.
- Caching prevents recomputation of expensive aggregations.
- Redundant window functions are minimized for better aggregation performance.

> This PR was generated automatically. Please run tests and review carefully.
