## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, using CTEs for predicate pushdown, and removing repeated subqueries to improve join and scan efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Filters are now applied before joins for better partition pruning and reduced data shuffling.
- Repeated subqueries are replaced with CTEs, reducing redundant computation.
- Broadcast hints are retained only for small tables, improving join performance.

> This PR was generated automatically. Please run tests and review carefully.
