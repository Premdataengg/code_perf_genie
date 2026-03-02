## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing filters early, pruning columns, using CTEs for reuse, and improving join strategies. These changes reduce data shuffling, avoid repeated computation, and leverage partition pruning.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Filter pushdown and CTEs reduce data scanned and shuffled
- Column pruning and early filtering improve memory and CPU efficiency
- Broadcast joins retained only for small tables

> This PR was generated automatically. Please run tests and review carefully.
