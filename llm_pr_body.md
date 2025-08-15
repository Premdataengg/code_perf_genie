## Daily Spark Optimization

**Summary**
Refactored repeated employee subqueries into a single CTE to reduce redundant scans and improve join performance.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates three identical subqueries by using a single CTE.
- Reduces query plan complexity and improves execution efficiency.

> This PR was generated automatically. Please run tests and review carefully.
