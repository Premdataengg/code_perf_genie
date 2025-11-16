## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by introducing partition pruning, predicate pushdown, join order improvements, CTE materialization for caching, column pruning, and aggregation simplification. These changes reduce data shuffling, avoid full table scans, and minimize redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Added partition and column pruning to avoid full scans
- Reordered joins and pushed down predicates for better partition pruning
- Materialized expensive CTEs to simulate caching and reduce recomputation
- Reduced redundant window and aggregation functions using CTEs
- Moved expensive subqueries to CTEs and pruned columns early

> This PR was generated automatically. Please run tests and review carefully.
