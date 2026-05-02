## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by enabling partition pruning, predicate pushdown, column pruning, and better join/filter order. No change to output, but queries now scan and process less data.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Predicate pushdown and join reordering improve query execution plans and reduce shuffle

> This PR was generated automatically. Please run tests and review carefully.
