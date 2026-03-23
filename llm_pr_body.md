## Daily Spark Optimization

**Summary**
Optimized repeated subqueries in anti-pattern file by introducing a CTE for employee filtering and pruning columns early.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Removes repeated SELECT * subqueries for employees, reducing scan and shuffle cost
- Improves join efficiency and reduces memory usage by pruning columns early

> This PR was generated automatically. Please run tests and review carefully.
