## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early, pruning columns, materializing expensive CTEs, optimizing join order, and reducing redundant aggregations and window functions.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce scanned data and shuffle.
- Broadcast joins and join order minimize shuffle and memory usage.
- Materializing intermediate results simulates caching for reused computations.
- Precomputing aggregations and window functions reduces redundant work.

> This PR was generated automatically. Please run tests and review carefully.
