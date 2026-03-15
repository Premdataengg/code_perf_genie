## Daily Spark Optimization

**Summary**
Optimized anti-pattern query by using a CTE for filtered employees and pruning columns early to reduce data scanned and avoid repeated subqueries.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates repeated subqueries for employee filtering
- Reduces data scanned and improves join efficiency

> This PR was generated automatically. Please run tests and review carefully.
