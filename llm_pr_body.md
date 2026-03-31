## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters for partition pruning, using CTEs to avoid repeated subqueries, and caching expensive intermediate results.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and predicate pushdown reduce data scanned and improve query speed.
- Caching expensive aggregations avoids recomputation, saving memory and CPU.
- Using CTEs for filtered data eliminates redundant scans and improves join efficiency.

> This PR was generated automatically. Please run tests and review carefully.
