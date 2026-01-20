## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, predicate pushdown, join order improvements, simulated caching, and precomputing aggregations to minimize redundant computation and data shuffling.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and shuffle
- Join order and predicate pushdown minimize data processed in joins
- Simulated caching avoids recomputation of expensive CTEs
- Precomputing aggregations reduces repeated window function costs

> This PR was generated automatically. Please run tests and review carefully.
