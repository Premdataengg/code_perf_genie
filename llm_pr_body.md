## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, predicate pushdown, join reordering, column pruning, and simulated caching of expensive intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition and column pruning reduce data scanned and shuffled.
- Predicate pushdown and join reordering minimize intermediate data size.
- Materializing expensive CTEs simulates caching, reducing recomputation.

> This PR was generated automatically. Please run tests and review carefully.
