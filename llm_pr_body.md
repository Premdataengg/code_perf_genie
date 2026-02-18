## Daily Spark Optimization

**Summary**
Materialized and cached the expensive_calc CTE to avoid repeated computation, improving performance for queries referencing this aggregation.

**Files updated**
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Introduced CREATE OR REPLACE TEMP VIEW and CACHE TABLE for expensive_calc
- Reduces redundant computation and speeds up downstream queries

> This PR was generated automatically. Please run tests and review carefully.
