## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, column pruning, broadcast hints, and simulated caching/materialization for expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Broadcast join hints and CTE materialization reduce join and aggregation costs.
- Performance gain: less memory usage, fewer shuffles, and lower compute time.

> This PR was generated automatically. Please run tests and review carefully.
