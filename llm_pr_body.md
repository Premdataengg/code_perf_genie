## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by eliminating redundant scans, pushing down predicates for partition pruning, improving join order and hints, and materializing expensive CTEs only once.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Eliminated repeated subqueries and redundant scans with CTEs
- Enabled partition pruning and predicate pushdown for large tables
- Added broadcast hints for small tables to optimize joins
- Materialized expensive aggregations to avoid recomputation

> This PR was generated automatically. Please run tests and review carefully.
