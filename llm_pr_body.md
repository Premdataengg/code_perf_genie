## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, join reordering, predicate pushdown, caching, and pre-aggregation to reduce data shuffling and recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and improve scan efficiency
- Join reordering and predicate pushdown minimize data movement and join costs
- Caching intermediate results avoids redundant computation
- Pre-aggregation and column selection reduce window function overhead and data shuffling

> This PR was generated automatically. Please run tests and review carefully.
