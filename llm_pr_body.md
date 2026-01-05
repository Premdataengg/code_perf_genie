## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, predicate pushdown, caching, column pruning, and reducing redundant window functions. These changes reduce data shuffling, avoid full table scans, and minimize recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition and column pruning reduces I/O and memory usage
- Predicate pushdown and join reordering minimize shuffle and improve join efficiency
- Caching expensive CTEs avoids recomputation and speeds up repeated access
- Precomputing and reusing aggregations reduces redundant computation in window functions

> This PR was generated automatically. Please run tests and review carefully.
