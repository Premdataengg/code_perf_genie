## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters for partition pruning, using CTEs to avoid repeated subqueries, and reordering joins for early filtering and efficient broadcast usage.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and early filtering reduce data scanned and improve query speed.
- CTEs eliminate redundant scans and enable Spark to optimize join order.
- Broadcasting is limited to small tables, reducing shuffle and memory usage.

> This PR was generated automatically. Please run tests and review carefully.
