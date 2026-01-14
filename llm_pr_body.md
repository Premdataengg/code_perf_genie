## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying column pruning, predicate pushdown, CTEs for expensive subqueries, efficient join strategies, and window function reductions. Broadcast hints and join order improvements were added for better performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column pruning and predicate pushdown reduce data scanned and shuffled
- Broadcast joins and CTEs avoid recomputation and improve join efficiency
- Window functions and aggregations are minimized and pre-aggregated where possible

> This PR was generated automatically. Please run tests and review carefully.
