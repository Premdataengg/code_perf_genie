## Daily Spark Optimization

**Summary**
Refactored repeated subqueries into a single CTE for employee filtering, reducing redundant scans and improving join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates repeated subqueries for employee filtering
- Reduces cartesian join size and improves predicate pushdown

> This PR was generated automatically. Please run tests and review carefully.
