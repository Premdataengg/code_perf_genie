## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing filters early, reordering joins, caching expensive CTEs, pruning columns, and reducing redundant window/aggregation operations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce scan and shuffle costs.
- Join reordering and early filtering minimize data movement.
- Caching avoids recomputation of expensive aggregations.
- Reducing window/aggregation passes improves CPU and memory efficiency.

> This PR was generated automatically. Please run tests and review carefully.
