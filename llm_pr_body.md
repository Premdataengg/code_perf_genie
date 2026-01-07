## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, predicate pushdown, join/filter order, column pruning, simulated caching, and reducing redundant window/aggregation passes.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage.
- Predicate pushdown and join order minimize data shuffling and computation.
- Simulated caching avoids recomputation of expensive aggregations.
- Combining window functions and precomputing aggregates reduces multiple data scans.

> This PR was generated automatically. Please run tests and review carefully.
