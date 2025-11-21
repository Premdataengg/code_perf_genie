## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns, materializing expensive CTEs, and reducing redundant window/aggregation operations. These changes improve partition pruning, reduce data shuffling, and minimize recomputation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Filter pushdown and column pruning reduce data scanned and shuffled
- Materializing CTEs and precomputing aggregates minimize recomputation and memory usage
- Broadcast hints and explicit JOIN order improve join performance

> This PR was generated automatically. Please run tests and review carefully.
