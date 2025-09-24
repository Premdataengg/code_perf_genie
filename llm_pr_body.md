## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by applying partition pruning, early filtering, column pruning, join order optimization, and reducing redundant computations and memory usage.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`


**Notes**
- Partition pruning and early filtering reduce data scanned and shuffled
- Column pruning and join order optimization minimize memory and CPU usage
- Redundant window functions and string/date operations removed for efficiency

> This PR was generated automatically. Please run tests and review carefully.
