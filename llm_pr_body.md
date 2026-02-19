## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by introducing partition pruning, broadcast hints, precomputed CTEs for aggregations, and early filtering to reduce data shuffling and memory usage.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Added partition pruning and LIMIT to reduce full table scans
- Moved filters before joins for predicate pushdown
- Introduced CTEs for expensive/repeated aggregations
- Added comments for caching and best practices
- Performance gain: less data shuffled, fewer recomputations, and lower memory usage

> This PR was generated automatically. Please run tests and review carefully.
