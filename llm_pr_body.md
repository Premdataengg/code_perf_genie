## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, using CTEs for predicate pushdown and caching, reordering joins, adding broadcast hints, and replacing subquery aggregations with joins to pre-aggregated CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Partition pruning and column pruning reduce data scanned and shuffled
- Broadcast and join reordering improve join efficiency
- Caching and CTEs avoid repeated computation and memory waste
- Replacing subquery aggregations with joins reduces memory and CPU usage

> This PR was generated automatically. Please run tests and review carefully.
