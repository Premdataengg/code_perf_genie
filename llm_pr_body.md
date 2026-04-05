## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, predicate pushdown, join reordering, column pruning, and caching of expensive aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition and column pruning reduce data scanned and shuffled.
- Join reordering and predicate pushdown filter large tables early, improving join efficiency.
- Caching expensive CTEs avoids recomputation and speeds up repeated aggregations.

> This PR was generated automatically. Please run tests and review carefully.
