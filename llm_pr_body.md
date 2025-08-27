## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by introducing CTEs for predicate pushdown, partition pruning, and materializing expensive computations to avoid recomputation and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Eliminated repeated subqueries with CTEs for better scan efficiency
- Enabled partition pruning and predicate pushdown for large tables
- Added broadcast hints and filtered joins for improved join performance
- Materialized expensive aggregations to mimic caching and reduce recomputation

> This PR was generated automatically. Please run tests and review carefully.
