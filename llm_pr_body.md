## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying early filtering, column pruning, efficient window and join strategies, and recommending caching for expensive aggregations. These changes reduce data shuffling, memory usage, and recomputation, resulting in improved Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filtering and column pruning reduce I/O and memory footprint
- Efficient join order and broadcast hints minimize shuffle and improve join speed
- Caching intermediate aggregations avoids redundant computation
- Refactored window and aggregation logic to minimize repeated work

> This PR was generated automatically. Please run tests and review carefully.
