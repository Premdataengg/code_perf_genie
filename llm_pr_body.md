## Daily Spark Optimization

**Summary**
Added a materialization step for the expensive_calc CTE to simulate caching, preventing repeated computation and improving query performance.

**Files updated**
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Materialized expensive_calc as a temp view to avoid recomputation
- Reduces redundant aggregation work and speeds up downstream queries

> This PR was generated automatically. Please run tests and review carefully.
