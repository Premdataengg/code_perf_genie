## Daily Spark Optimization

**Summary**
Optimized all anti-pattern SQL files by pushing down filters, pruning columns, using CTEs for filtered data, and reducing redundant computation, especially for joins and aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and shuffled
- Materializing expensive CTEs simulates caching and avoids recomputation
- Window functions and aggregations are streamlined for fewer passes
- Broadcast hints are retained only for small tables

> This PR was generated automatically. Please run tests and review carefully.
