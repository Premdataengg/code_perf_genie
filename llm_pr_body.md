## Daily Spark Optimization

**Summary**
Refactored repeated subqueries in anti-pattern 6 into a single CTE for filtered employees, reducing redundant scans and improving join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates multiple identical subqueries by using a CTE.
- Reduces full table scans and improves query plan for large datasets.

> This PR was generated automatically. Please run tests and review carefully.
