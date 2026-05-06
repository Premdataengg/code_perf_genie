## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing filters before joins, pruning columns early, reordering joins, and adding caching hints for expensive aggregations. These changes improve partition pruning, reduce shuffle, and lower memory usage while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Early filtering and column pruning reduce data scanned and shuffled.
- Join order and broadcast hints minimize shuffle and improve join efficiency.
- Window and aggregation optimizations reduce computation and memory footprint.
- Caching suggestions prevent recomputation of expensive intermediate results.

> This PR was generated automatically. Please run tests and review carefully.
