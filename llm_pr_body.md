## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join order, caching, and aggregation strategies. Redundant computations and unnecessary data scans were eliminated for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Added column pruning and efficient window frames for partitioning
- Reordered joins and pushed down predicates for join optimization
- Added caching comments for reused aggregates
- Pre-aggregated window functions and reduced redundant calculations

> This PR was generated automatically. Please run tests and review carefully.
