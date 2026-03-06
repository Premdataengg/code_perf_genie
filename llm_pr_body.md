## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by introducing partition pruning, column selection, broadcast/caching hints, and early filter application to reduce I/O and recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column selection reduce scan and shuffle costs.
- Broadcast and cache hints improve join and aggregation performance.

> This PR was generated automatically. Please run tests and review carefully.
