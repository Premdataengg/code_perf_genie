## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by enabling partition pruning, broadcasting small tables, caching expensive aggregations, and restructuring joins to avoid cartesian products and redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Broadcast joins and caching improve join and aggregation performance
- CTEs and EXISTS reduce redundant computation and avoid cartesian joins

> This PR was generated automatically. Please run tests and review carefully.
