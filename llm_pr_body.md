## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by enabling partition pruning, predicate pushdown, column pruning, join broadcast hints, and caching of expensive CTEs. Reduced redundant window functions and filtered early for better performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Broadcast hints and predicate pushdown improve join and filter efficiency
- Caching avoids recomputation of expensive aggregations
- Window function reduction minimizes computation overhead

> This PR was generated automatically. Please run tests and review carefully.
