## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join order, predicate pushdown, caching, and aggregation strategies for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Added partition pruning and column selection to reduce I/O
- Reordered joins and pushed down filters for less data shuffling
- Used MATERIALIZED CTE to simulate caching of expensive aggregations
- Precomputed aggregations in CTEs to minimize redundant window function computation

> This PR was generated automatically. Please run tests and review carefully.
