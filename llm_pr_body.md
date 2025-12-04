## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, reordering joins, simulating caching with CTEs, pruning columns, and reducing redundant window/aggregation operations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Join reordering and early filtering minimize join size and improve execution plan.
- Simulated caching via CTEs avoids recomputation of expensive aggregations.
- Window and aggregation optimizations reduce computation and memory overhead.

> This PR was generated automatically. Please run tests and review carefully.
