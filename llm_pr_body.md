## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, efficient join order, column pruning, predicate pushdown, and caching of expensive aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce scanned data and memory usage.
- Join reordering and predicate pushdown minimize shuffle and intermediate data.
- Caching expensive aggregations avoids recomputation and speeds up repeated access.

> This PR was generated automatically. Please run tests and review carefully.
