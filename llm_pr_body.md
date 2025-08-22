## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by reducing redundant scans, enabling partition pruning, pushing down filters, and improving join strategies using CTEs and broadcast hints.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Eliminated repeated subqueries with CTEs for better scan efficiency
- Moved partition-pruning predicates to WHERE for early filtering
- Pushed down filters before joins to reduce shuffle and data volume
- Added broadcast hint to projects join for small tables

> This PR was generated automatically. Please run tests and review carefully.
