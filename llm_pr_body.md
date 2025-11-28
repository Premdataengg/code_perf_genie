## Daily Spark Optimization

**Summary**
Optimized all anti-pattern queries by pushing down filters, pruning columns, improving join order, simulating caching, and reducing redundant computations for better Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Join order and broadcast hints minimize data shuffling
- Materializing CTEs simulates caching for reused computations
- Redundant window and aggregation functions are reduced for efficiency

> This PR was generated automatically. Please run tests and review carefully.
