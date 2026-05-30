## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by eliminating redundant subqueries, pushing down predicates, and pruning columns early to reduce data scans and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`


**Notes**
- Removed repeated subqueries in multi-way joins using a CTE for employee filtering
- Applied predicate pushdown and column pruning for partition pruning and reduced I/O

> This PR was generated automatically. Please run tests and review carefully.
