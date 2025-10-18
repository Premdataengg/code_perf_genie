## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, reducing redundant scans with CTEs, and pushing down predicates before joins for better performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and improve query speed
- CTEs avoid repeated subqueries and redundant scans
- Predicate pushdown before joins reduces shuffle and improves join efficiency

> This PR was generated automatically. Please run tests and review carefully.
