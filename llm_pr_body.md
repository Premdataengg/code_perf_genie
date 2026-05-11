## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by enabling partition pruning, predicate pushdown, column pruning, join reordering, broadcast hints, caching, and replacing correlated subqueries with pre-aggregated joins.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Partition pruning and predicate pushdown reduce scanned data and improve IO.
- Broadcast joins and caching reduce shuffle and recomputation.
- Column pruning and pre-aggregation reduce memory and computation overhead.

> This PR was generated automatically. Please run tests and review carefully.
