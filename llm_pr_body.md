## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing filters early, pruning columns, reordering joins, precomputing aggregates in CTEs, and simulating caching to reduce recomputation and improve Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Pushed filters and column selection into CTEs for partition pruning and predicate pushdown
- Reordered joins and added broadcast hints only for small tables
- Precomputed aggregates and reused them to avoid redundant window and aggregation computations
- Replaced subqueries with joins to precomputed CTEs for memory efficiency
- Performance gain: reduced full scans, shuffles, and recomputation; improved query execution plans

> This PR was generated automatically. Please run tests and review carefully.
