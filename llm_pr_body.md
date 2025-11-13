## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by enabling partition pruning, pushing down filters, reducing redundant window functions, adding caching hints, and reordering joins for better performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and improve partition pruning.
- Broadcast and CACHED hints, as well as join reordering, minimize shuffling and recomputation.
- Redundant window functions removed to reduce computation overhead.

> This PR was generated automatically. Please run tests and review carefully.
