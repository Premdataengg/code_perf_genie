## Daily Spark Optimization

**Summary**
Added caching for the expensive_calc CTE to avoid recomputation and improve query performance.

**Files updated**
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Caches expensive intermediate aggregation for reuse
- Reduces redundant computation and speeds up downstream queries

> This PR was generated automatically. Please run tests and review carefully.
