## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, predicate pushdown, join reordering, column pruning, window function simplification, and simulated caching for expensive aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled
- Join reordering and predicate pushdown improve join efficiency
- Materializing expensive CTEs simulates caching and avoids recomputation
- Window function usage reduced to only necessary calculations

> This PR was generated automatically. Please run tests and review carefully.
