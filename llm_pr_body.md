## Daily Spark Optimization

**Summary**
Optimized the most inefficient anti-pattern by removing repeated subqueries using a CTE and enabling predicate pushdown for employee filtering.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates multiple full scans of the employees table
- Reduces computation and improves join efficiency

> This PR was generated automatically. Please run tests and review carefully.
