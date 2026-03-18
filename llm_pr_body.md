## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by adding partition pruning, filter pushdown, broadcast join hints, and caching of expensive aggregations. These changes reduce full scans, shuffle, and recomputation for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Added partition pruning and filter pushdown for large tables
- Ensured broadcast join for small dimension tables
- Introduced CACHE hint for expensive aggregations
- Performance gain: less data scanned, reduced shuffle, and faster repeated aggregations

> This PR was generated automatically. Please run tests and review carefully.
