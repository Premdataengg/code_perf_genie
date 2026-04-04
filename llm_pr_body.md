## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, column pruning, predicate pushdown, and CTEs to reduce redundant scans and improve join efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled
- Predicate pushdown and CTEs minimize redundant computation and improve join performance

> This PR was generated automatically. Please run tests and review carefully.
