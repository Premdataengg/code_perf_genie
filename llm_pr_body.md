## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by reducing repeated subqueries, improving join and filter order, and simulating caching of expensive aggregations using CTEs.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Reused filtered employee set via CTE to avoid repeated scans and cartesian joins.
- Moved filter predicates into CTEs for better partition pruning and predicate pushdown.
- Ensured expensive aggregations are computed once and reused, simulating caching.
- Broadcast hints retained for small dimension tables.

> This PR was generated automatically. Please run tests and review carefully.
