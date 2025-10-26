## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by introducing partition pruning, column pruning, predicate pushdown, efficient join order, and simulated caching of expensive intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Predicate pushdown and join reordering minimize join input size.
- Simulated caching avoids recomputation of expensive aggregations, improving query performance.

> This PR was generated automatically. Please run tests and review carefully.
