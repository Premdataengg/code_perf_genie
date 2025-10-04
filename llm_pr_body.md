## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by improving partition pruning, join strategies, caching, and aggregation efficiency. Key changes include column pruning, broadcast hints, predicate pushdown, and window function optimizations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Early column and predicate pruning reduces data scanned and shuffled.
- Broadcast joins and join reordering improve join performance.
- Window and aggregation optimizations reduce multiple passes over data.
- Simulated caching with CTEs avoids recomputation of expensive subqueries.

> This PR was generated automatically. Please run tests and review carefully.
