## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early, reducing data scanned, removing redundant subqueries, and improving join strategies for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Pushed filters into CTEs for partition pruning and predicate pushdown
- Removed SELECT * and selected only needed columns
- Reduced redundant subqueries and improved join order
- Broadcasted only small tables to minimize shuffle

> This PR was generated automatically. Please run tests and review carefully.
