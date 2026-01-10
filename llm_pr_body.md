## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pruning columns, pushing filters into CTEs for predicate pushdown, adding caching hints, and restructuring joins and aggregations for efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column pruning and predicate pushdown reduce data scanned and improve partition pruning.
- Caching and pre-aggregated CTEs avoid recomputation and speed up repeated calculations.
- Join and aggregation optimizations reduce shuffle and computation overhead.

> This PR was generated automatically. Please run tests and review carefully.
