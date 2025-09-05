## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, predicate pushdown, column pruning, and simulating caching of expensive aggregations. Joins and filters are now more efficient, reducing data scanned and computation time.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage
- Predicate pushdown and early filtering minimize data shuffled in joins
- Materializing expensive CTEs simulates caching, avoiding redundant computation

> This PR was generated automatically. Please run tests and review carefully.
