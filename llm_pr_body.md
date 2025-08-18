## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by eliminating redundant subqueries, enabling partition pruning, and improving join order and filtering for Spark SQL.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Removed repeated subqueries in multi-join by using a CTE for filtered employees.
- Moved partition-pruning predicates to WHERE clause for dynamic partition pruning.
- Added broadcast hint and pushed down filters before joins for better join performance.

> This PR was generated automatically. Please run tests and review carefully.
