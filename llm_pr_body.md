## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters for partition pruning, reordering joins, materializing expensive CTEs to simulate caching, and reducing redundant window functions and aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage
- Join reordering and predicate pushdown minimize shuffle and data scanned
- Materializing CTEs simulates caching, reducing recomputation
- Precomputing aggregations and reducing window function redundancy improves query execution speed

> This PR was generated automatically. Please run tests and review carefully.
