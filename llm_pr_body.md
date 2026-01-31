## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by applying partition pruning, column pruning, early filtering, efficient join strategies, caching hints, and reducing redundant window functions for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Broadcast join hints and early filtering minimize shuffle and memory usage.
- Caching intermediate aggregations avoids recomputation.
- Redundant window functions are consolidated to reduce computation overhead.

> This PR was generated automatically. Please run tests and review carefully.
