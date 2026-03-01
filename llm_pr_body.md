## Daily Spark Optimization

**Summary**
Optimized the worst anti-pattern query by removing repeated subqueries and using a CTE for employee filtering, reducing data scans and improving join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminated repeated subqueries for employee filtering
- Reduced cartesian join cost and improved column pruning

> This PR was generated automatically. Please run tests and review carefully.
