## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by improving partition pruning, join order, predicate pushdown, column pruning, and by recommending caching of reused intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce full table scans and memory usage.
- Early filtering and join reordering minimize shuffle and improve join performance.
- Caching intermediate results avoids recomputation and speeds up repeated aggregations.

> This PR was generated automatically. Please run tests and review carefully.
