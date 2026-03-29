## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, reducing repeated scans with CTEs, and simulating caching of expensive aggregations. These changes improve scan efficiency and reduce redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled
- CTE reuse avoids repeated expensive scans and mimics caching
- Performance gain from less data movement and fewer recomputations

> This PR was generated automatically. Please run tests and review carefully.
