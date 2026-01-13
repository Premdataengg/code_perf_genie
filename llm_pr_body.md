## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, column pruning, efficient window frames, join reordering, broadcast hints, predicate pushdown, and caching strategies for intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage.
- Efficient window frames and join order improve query execution speed.
- Caching intermediate aggregations avoids recomputation and speeds up repeated use.

> This PR was generated automatically. Please run tests and review carefully.
