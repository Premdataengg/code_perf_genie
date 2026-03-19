## Daily Spark Optimization

**Summary**
Optimized anti-pattern query by introducing a CTE for filtered employees, reducing repeated scans and improving join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates repeated subqueries and cartesian joins
- Pushes down filters and prunes columns for performance gain

> This PR was generated automatically. Please run tests and review carefully.
