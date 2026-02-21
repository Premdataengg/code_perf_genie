## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by adding partition pruning, pushing down filters, reordering joins, reducing window function usage, and simulating caching via CTE reuse. These changes minimize data shuffling, avoid full scans, and reduce repeated computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage.
- Join order and filter pushdown minimize shuffle and improve join performance.
- CTE reuse simulates caching, avoiding recomputation of expensive aggregations.
- Precomputing aggregations and pruning columns reduce window function overhead.

> This PR was generated automatically. Please run tests and review carefully.
