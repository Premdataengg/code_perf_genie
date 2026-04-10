## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, pushing down predicates, adding broadcast/caching hints, and reducing redundant window/aggregation operations. These changes improve Spark SQL performance while preserving output.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce data scanned and shuffled.
- Broadcast and cache hints minimize data movement and recomputation.
- Window function consolidation reduces multiple passes over data.

> This PR was generated automatically. Please run tests and review carefully.
