## Daily Spark Optimization

**Summary**
Added a CACHE hint to the expensive_calc CTE to prevent recomputation and improve performance for repeated aggregations.

**Files updated**
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Introduced Spark SQL /*+ CACHE */ hint for expensive_calc CTE.
- Reduces redundant computation and improves query execution time when intermediate results are reused.

> This PR was generated automatically. Please run tests and review carefully.
