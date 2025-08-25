## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by eliminating repeated subqueries, enabling partition pruning, and simulating caching with CTEs to reduce recomputation and improve Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Removed repeated subqueries by using CTEs for filtered employees and expensive aggregations.
- Moved partition-pruning predicates to main query for dynamic partition pruning.
- Performance gain: fewer data scans, better join efficiency, and reduced recomputation.

> This PR was generated automatically. Please run tests and review carefully.
