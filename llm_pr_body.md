## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters before joins for predicate pushdown, selecting only needed columns, caching expensive CTEs, and reducing window function passes.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Predicate pushdown and CTE caching minimize recomputation and speed up query execution
- Broadcast hints and filter pushdown improve join performance

> This PR was generated automatically. Please run tests and review carefully.
