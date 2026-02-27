## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by improving partition pruning, join strategies, and simulating caching of expensive intermediate results. These changes reduce unnecessary full scans, optimize join execution, and avoid recomputation of costly aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Added partition pruning and filter pushdown for large_dataset queries
- Introduced BROADCAST hint for small tables and reordered joins for better join performance
- Simulated caching of expensive CTEs to avoid repeated computation and improve reuse

> This PR was generated automatically. Please run tests and review carefully.
