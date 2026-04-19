## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns, reordering joins, adding broadcast hints, and using CTEs for reuse and partition pruning. Subquery aggregations were replaced with precomputed CTEs to reduce repeated computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter pushdown and column pruning reduce data scanned and shuffled.
- Broadcast hints and join reordering minimize shuffle and improve join efficiency.
- Precomputing aggregations and using CTEs avoids repeated expensive calculations.
- Partition pruning and avoiding cartesian joins significantly improve performance.

> This PR was generated automatically. Please run tests and review carefully.
