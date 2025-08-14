## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by factoring out repeated subqueries, improving join strategies with broadcast hints, and enabling partition pruning.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`


**Notes**
- Factored repeated employee filters into a CTE to avoid redundant scans and joins.
- Added broadcast hint to projects table and reordered joins for better join performance.
- Moved filter predicates into CTE for partition pruning, reducing full table scans.

> This PR was generated automatically. Please run tests and review carefully.
