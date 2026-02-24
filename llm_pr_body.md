## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, predicate pushdown, efficient join strategies, caching hints, and reducing redundant window functions and aggregations. These changes improve scan, join, and aggregation performance while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce IO and memory usage
- Early filtering and broadcast joins minimize shuffle and join cost
- Caching intermediate results avoids recomputation
- Window and aggregation optimizations reduce computation overhead

> This PR was generated automatically. Please run tests and review carefully.
