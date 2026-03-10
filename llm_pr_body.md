## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by removing redundant scans, enabling partition pruning, pushing down predicates, and simulating caching with CTEs. Broadcast hints are used only for small tables.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Removed repeated subqueries with CTEs for filtered employees.
- Moved partition-pruning filters into CTEs for efficient scans.
- Added broadcast hints for small tables and pushed filters before joins.
- Simulated caching by referencing expensive aggregations only once via CTE.

> This PR was generated automatically. Please run tests and review carefully.
