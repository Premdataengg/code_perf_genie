## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by removing repeated subqueries, enabling partition pruning, and pushing down predicates before joins for better performance.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Replaced repeated subqueries with a CTE for filtered employees to reduce scans and improve join efficiency.
- Moved filter predicates into main SELECTs and before joins to enable partition pruning and predicate pushdown.
- These changes reduce data shuffling, avoid unnecessary full table scans, and improve overall query execution time.

> This PR was generated automatically. Please run tests and review carefully.
