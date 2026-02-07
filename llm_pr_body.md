## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, column pruning, efficient join order, broadcast hints, and precomputing aggregations to reduce data shuffling and recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage
- Join order and predicate pushdown minimize shuffle and improve join efficiency
- Precomputing and reusing aggregations/window functions avoids redundant computation

> This PR was generated automatically. Please run tests and review carefully.
