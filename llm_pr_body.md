## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by reducing redundant scans, enabling partition pruning, improving join strategies, and simulating caching of expensive aggregations using CTEs.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Eliminated repeated subqueries and enabled predicate pushdown for better scan efficiency.
- Added broadcast hints and reordered joins for improved join performance.
- Used CTEs to simulate caching and avoid recomputation of expensive aggregations.

> This PR was generated automatically. Please run tests and review carefully.
