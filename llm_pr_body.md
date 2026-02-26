## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by eliminating repeated subqueries, enabling partition pruning, and improving join strategies with broadcast hints and early predicate pushdown.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Eliminated repeated scans of employees table with a CTE in performance anti-pattern query.
- Enabled partition pruning by pushing filters into the main query.
- Added BROADCAST hint to projects join and pushed predicates before joins for efficient join order.

> This PR was generated automatically. Please run tests and review carefully.
