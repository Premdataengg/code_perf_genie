## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early for partition pruning, using CTEs to avoid repeated subqueries, and pruning columns before joins. Broadcast hints are retained only for small tables.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and predicate pushdown reduce data scanned and improve query speed.
- CTEs and column pruning minimize repeated computation and memory usage.
- Broadcast join hints are used only for small tables, preventing inefficient shuffles.

> This PR was generated automatically. Please run tests and review carefully.
