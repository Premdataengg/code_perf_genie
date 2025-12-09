## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by adding partition pruning, column pruning, predicate pushdown, efficient join order, CTE reuse, and aggregation precomputation. This reduces data scanned, shuffles, and repeated computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition and column pruning reduces I/O and memory usage
- Predicate pushdown and CTE reuse minimize data shuffling and recomputation
- Efficient join order and broadcast hints improve join performance
- Precomputing aggregations in CTEs reduces redundant window calculations

> This PR was generated automatically. Please run tests and review carefully.
