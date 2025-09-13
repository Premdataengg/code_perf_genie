## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by enabling partition pruning, pushing down filters, projecting only needed columns, and simulating caching for expensive CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage.
- Filter pushdown and CTE refactoring minimize data shuffled and recomputation.
- Broadcast join hints are preserved only for small tables.

> This PR was generated automatically. Please run tests and review carefully.
