## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying partition pruning, predicate pushdown, join and aggregation best practices, and simulated caching for expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Join order and predicate pushdown minimize unnecessary computation and improve join efficiency.
- Materializing CTEs simulates caching to avoid recomputation.
- Precomputing aggregations and minimizing window partitions reduces redundant calculations and improves performance.

> This PR was generated automatically. Please run tests and review carefully.
