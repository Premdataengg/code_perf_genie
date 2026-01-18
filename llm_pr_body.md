## Daily Spark Optimization

**Summary**
Optimized all anti-pattern SQL files by pushing filters into CTEs, materializing expensive computations, reducing redundant window functions, and improving join strategies for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter pushdown and CTE materialization reduce data scanned and recomputation.
- Broadcast hints and optimized join order minimize shuffle and memory usage.
- Column pruning and aggregation precomputation reduce memory footprint and improve query speed.

> This PR was generated automatically. Please run tests and review carefully.
