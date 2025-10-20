## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing filters into subqueries/CTEs, applying column pruning, and improving join/filter order for better predicate pushdown and partition pruning.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Moved filters and projections as early as possible for predicate and column pruning
- Reordered joins and added broadcast hints for join optimization
- Encouraged caching of expensive CTEs where reused
- Performance gain: less data scanned, fewer shuffles, and more efficient execution plans

> This PR was generated automatically. Please run tests and review carefully.
