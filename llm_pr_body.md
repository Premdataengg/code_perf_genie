## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by adding partition pruning, column pruning, caching via MATERIALIZED CTEs, pushing down filters, consolidating window functions, and using CTEs for repeated subqueries. These changes improve join efficiency, reduce memory usage, and minimize data shuffling.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce scan and I/O costs
- Broadcast hints and join reordering improve join performance
- Caching/MATERIALIZED CTEs avoid repeated expensive computations
- Consolidated window functions and pre-aggregation reduce shuffle and computation

> This PR was generated automatically. Please run tests and review carefully.
