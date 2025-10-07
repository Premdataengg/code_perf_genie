## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by applying partition pruning, column pruning, join filtering, CTE-based caching, and aggregation precomputation to reduce data shuffling and repeated computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Join filtering and CTEs minimize shuffle and recomputation
- Aggregation precomputation avoids redundant window calculations

> This PR was generated automatically. Please run tests and review carefully.
