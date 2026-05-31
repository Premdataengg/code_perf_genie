## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by reducing redundant scans, improving join order, and pushing filters down for better partition pruning and join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Eliminated repeated subqueries with CTEs for filtered employees.
- Moved filters before joins and applied partition pruning directly.
- Reordered joins and used CTEs to filter large tables early, improving performance.

> This PR was generated automatically. Please run tests and review carefully.
