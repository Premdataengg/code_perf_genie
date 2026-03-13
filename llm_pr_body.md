## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by improving partition pruning, join strategies, and simulating caching of expensive aggregations. These changes reduce full table scans, shuffle, and recomputation for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Added partition pruning and efficient window frames to avoid full table scans.
- Introduced BROADCAST hint and join reordering for more efficient joins.
- Simulated caching of expensive aggregations using CTE materialization.

> This PR was generated automatically. Please run tests and review carefully.
