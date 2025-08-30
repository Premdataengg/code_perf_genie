## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters for partition pruning, broadcasting small tables, and reducing redundant scans and joins.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and filter pushdown reduce full table scans
- Broadcasting small dimension tables improves join performance
- CTEs and filter reuse reduce redundant computation

> This PR was generated automatically. Please run tests and review carefully.
