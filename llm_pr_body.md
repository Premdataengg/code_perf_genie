## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by enabling partition pruning, predicate pushdown, efficient join strategies, window/aggregation reduction, and memory-efficient CTEs. All outputs remain identical.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled.
- Broadcast hints and join reordering improve join performance.
- Caching/materializing expensive CTEs avoids recomputation.
- Aggregations and window functions are minimized for efficiency.
- String/date operations and subqueries are moved to CTEs for reuse and memory savings.

> This PR was generated automatically. Please run tests and review carefully.
