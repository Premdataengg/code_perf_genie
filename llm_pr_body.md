## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by reducing redundant scans, enabling partition pruning, improving join order, and materializing expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Materialized filtered employees and aggregations to avoid recomputation
- Moved partition-pruning filters to main query for efficient scan
- Added broadcast hint to projects and reordered joins for better join performance
- Column pruning and CTEs reduce data shuffle and improve cache utilization

> This PR was generated automatically. Please run tests and review carefully.
