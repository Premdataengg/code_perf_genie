## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pruning columns, pushing down filters, precomputing aggregations, simulating caching, and improving join and window strategies for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Column pruning and filter pushdown reduce data scanned and shuffled
- Precomputing aggregations and simulating caching avoid repeated expensive computations
- Join order and broadcast hints improve join efficiency
- Window function optimizations reduce redundant computation
- Replacing subqueries with joins and EXISTS reduces repeated scans and memory usage

> This PR was generated automatically. Please run tests and review carefully.
