## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters into subqueries/CTEs, reducing data scanned and joined, and avoiding repeated subqueries. Improved partition pruning, join efficiency, and aggregation reuse.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and filter pushdown reduce full table scans and join sizes
- CTEs prevent repeated computation and enable Spark to optimize execution plans
- Column pruning avoids unnecessary data movement

> This PR was generated automatically. Please run tests and review carefully.
