## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns, reordering joins, reducing window function passes, and factoring out repeated subqueries using CTEs. Broadcast hints are used only for small tables, and expensive computations are minimized.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Filter and column pushdown reduces data scanned and shuffled
- Join order and broadcast hints improve join performance
- Window and aggregation optimizations reduce computation and memory usage
- Factoring out subqueries and using CTEs simulates caching and avoids recomputation

> This PR was generated automatically. Please run tests and review carefully.
