## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by removing redundant subqueries, enabling partition pruning, and pushing down predicates before joins for better performance.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Replaced repeated subqueries with a CTE for filtered employees (06_performance_anti_pattern.sql)
- Moved filters into main SELECT for partition pruning (07_anti_pattern_1_no_partitioning.sql)
- Applied predicate pushdown before joins and retained broadcast hint (08_anti_pattern_2_inefficient_joins.sql)

> This PR was generated automatically. Please run tests and review carefully.
