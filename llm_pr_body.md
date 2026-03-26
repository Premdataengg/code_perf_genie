## Daily Spark Optimization

**Summary**
Optimized anti-pattern query by removing repeated subqueries and cartesian joins, using CTEs and filtered joins for efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates repeated full scans of employees and departments.
- Reduces row explosion and leverages broadcast joins for small dimension tables.

> This PR was generated automatically. Please run tests and review carefully.
