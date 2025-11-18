## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, predicate pushdown, join reordering, column pruning, caching, and reducing window function complexity. These changes improve query execution efficiency and resource utilization while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Added column pruning and partition pruning for large scans
- Reordered joins and filtered large tables early for better join performance
- Added caching hints to avoid recomputation of expensive aggregations
- Reduced number of window functions and used CTEs for pre-aggregation

> This PR was generated automatically. Please run tests and review carefully.
