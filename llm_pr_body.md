## Daily Spark Optimization

**Summary**
Refactored repeated employee subqueries into a single CTE for predicate pushdown and reduced redundant scans.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Eliminates three identical subqueries by using a CTE.
- Reduces scan cost and enables Spark to optimize the join order.

> This PR was generated automatically. Please run tests and review carefully.
