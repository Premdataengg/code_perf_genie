## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join strategies, and suggesting caching for expensive aggregations. These changes reduce data scanned, shuffle, and recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Added partition pruning and LIMIT to avoid full scans
- Introduced BROADCAST hint for small join tables
- Suggested caching for expensive intermediate aggregations

> This PR was generated automatically. Please run tests and review carefully.
