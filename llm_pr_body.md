## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, column pruning, CTEs for filter reuse, join reordering, and predicate pushdown for improved Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and memory usage
- CTEs and join reordering minimize redundant computation and improve join efficiency

> This PR was generated automatically. Please run tests and review carefully.
