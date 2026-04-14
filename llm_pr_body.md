## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by reducing redundant scans, pushing filters for partition pruning, reordering joins, and using CTEs for predicate pushdown and column pruning.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Factored repeated subqueries into CTEs for employees in 06_performance_anti_pattern.sql
- Pushed partitionable filters and selected columns only in 07_anti_pattern_1_no_partitioning.sql
- Reordered joins, pushed filters, and used CTEs for early pruning in 08_anti_pattern_2_inefficient_joins.sql

> This PR was generated automatically. Please run tests and review carefully.
