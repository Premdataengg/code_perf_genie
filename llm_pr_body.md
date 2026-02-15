## Daily Spark Optimization

**Summary**
Materialized the expensive_calc CTE as a temp view to simulate caching, reducing redundant computation and improving performance.

**Files updated**
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Introduced temp view to simulate DataFrame caching in Spark SQL
- Avoids recomputation of expensive aggregations, improving query efficiency

> This PR was generated automatically. Please run tests and review carefully.
