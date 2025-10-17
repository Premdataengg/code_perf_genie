## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by adding partition pruning, predicate pushdown, join reordering, CTE reuse, and column pruning to reduce data shuffling and redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage
- Join order and predicate pushdown minimize shuffle and improve join efficiency
- CTE reuse simulates caching, reducing repeated expensive computations
- Window and aggregation optimizations reduce number of passes over data

> This PR was generated automatically. Please run tests and review carefully.
