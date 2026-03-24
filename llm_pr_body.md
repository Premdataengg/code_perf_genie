## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by removing repeated subqueries, improving join strategies, enabling partition pruning, and using CTEs for expensive computations.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Eliminated repeated subqueries and enabled predicate pushdown for better partition pruning.
- Added broadcast hints and improved join order for more efficient joins.
- Used CTEs to avoid recomputation of expensive aggregations and pruned columns.

> This PR was generated automatically. Please run tests and review carefully.
