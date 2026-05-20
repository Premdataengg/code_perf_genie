## Daily Spark Optimization

**Summary**
Optimized inefficient queries by applying partition pruning, predicate pushdown, column pruning, and better join ordering. Added CTEs and broadcast hints for improved performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition pruning and column selection reduce I/O and memory usage
- Early filtering and join reordering minimize data shuffling and improve execution speed

> This PR was generated automatically. Please run tests and review carefully.
