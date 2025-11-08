## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by adding column pruning, predicate pushdown, join reordering, caching hints, and reducing redundant window functions. These changes reduce data scanned, shuffle, and computation for improved Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and predicate pushdown reduce I/O and enable partition pruning.
- Join reordering and early filtering minimize shuffle and data movement.
- Caching intermediate results prevents recomputation of expensive aggregations.
- Window function optimizations reduce redundant calculations and memory usage.

> This PR was generated automatically. Please run tests and review carefully.
