## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by reducing redundant subqueries with CTEs and enabling partition pruning and column pruning for more efficient scans.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`


**Notes**
- Eliminated repeated subqueries in multi-join query by using a filtered CTE, reducing scan cost.
- Moved filters directly into main query to enable partition pruning and avoid unnecessary full table scans.

> This PR was generated automatically. Please run tests and review carefully.
