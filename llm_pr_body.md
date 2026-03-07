## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join efficiency, and simulating caching of expensive intermediate results. These changes reduce unnecessary scans, improve join performance, and avoid recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Added partition pruning and column selection to minimize data scanned
- Applied BROADCAST hints and pushed predicates into join conditions for efficient joins
- Indicated caching for expensive CTEs to avoid recomputation and improve reuse

> This PR was generated automatically. Please run tests and review carefully.
