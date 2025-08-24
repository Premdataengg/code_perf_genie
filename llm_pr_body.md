## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by reducing repeated scans, pushing filters into CTEs for partition pruning, and improving join efficiency with predicate pushdown and CTE materialization.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Eliminated redundant subqueries and repeated scans using CTEs
- Pushed filters into CTEs for better partition pruning and reduced join sizes
- Improved join order and ensured broadcast only on small tables
- Materialized expensive aggregations to mimic caching and avoid recomputation

> This PR was generated automatically. Please run tests and review carefully.
