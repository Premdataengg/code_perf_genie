## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by introducing early filtering, column pruning, partition pruning, broadcast hints, and CTEs for intermediate results. These changes improve join efficiency, aggregation performance, and memory usage while preserving query results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Early filtering and column pruning reduce data scanned and shuffled.
- Broadcast hints and join reordering optimize join strategies.
- Caching and efficient window/aggregation functions improve memory and compute efficiency.

> This PR was generated automatically. Please run tests and review carefully.
