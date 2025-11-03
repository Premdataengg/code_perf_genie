## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding column pruning, predicate pushdown, join reordering, window frame specification, and explicit caching comments. These changes reduce I/O, data shuffling, and redundant computation for significant performance gains.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Column pruning and filter pushdown minimize data scanned and shuffled
- Join reordering and broadcast hints improve join efficiency
- Window frame and aggregation precomputation reduce multiple passes over data
- Caching intermediate results avoids redundant expensive computations

> This PR was generated automatically. Please run tests and review carefully.
