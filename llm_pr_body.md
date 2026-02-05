## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing filters early for partition/pruning, pruning columns, adding broadcast/caching hints, and precomputing aggregations in CTEs to reduce recomputation and improve Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter pushdown and column pruning reduce data scanned and shuffled
- Broadcast/caching hints and CTE-based aggregation reuse minimize recomputation and memory usage
- Performance gain: less I/O, reduced shuffle, and more efficient execution plans

> This PR was generated automatically. Please run tests and review carefully.
