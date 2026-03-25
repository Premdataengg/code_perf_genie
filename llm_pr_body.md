## Daily Spark Optimization

**Summary**
Optimized the worst anti-pattern query by using CTEs to avoid repeated subqueries, pushing down filters, and deduplicating employee sets before joining, reducing cartesian join explosion.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates repeated subqueries and cartesian joins with CTEs and deduplication
- Significant performance gain by reducing data shuffling and join complexity

> This PR was generated automatically. Please run tests and review carefully.
