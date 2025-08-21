## Daily Spark Optimization

**Summary**
Refactored repeated subqueries into a single CTE for filtered employees to reduce redundant scans and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Replaced three identical subqueries with a CTE and referenced it multiple times.
- Reduces data scan and planning overhead, improving performance while preserving output.

> This PR was generated automatically. Please run tests and review carefully.
