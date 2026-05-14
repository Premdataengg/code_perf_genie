## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns, adding caching hints, and restructuring joins and aggregations for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Broadcast hints and join reordering improve join efficiency
- Caching intermediate results avoids recomputation
- Pre-aggregating in CTEs and using APPROX_COUNT_DISTINCT reduces shuffle and memory pressure

> This PR was generated automatically. Please run tests and review carefully.
