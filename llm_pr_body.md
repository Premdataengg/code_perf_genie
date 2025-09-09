## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing filters early, reducing cartesian joins, broadcasting small tables, and simulating caching with CTEs. These changes improve partition pruning, join efficiency, and reduce unnecessary data shuffling.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter pushdown enables partition pruning and reduces data scanned.
- Broadcasting small tables and using CTEs for reuse minimizes shuffle and recomputation.
- Avoiding cartesian joins and selecting only needed columns improves query execution time.

> This PR was generated automatically. Please run tests and review carefully.
