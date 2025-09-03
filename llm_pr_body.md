## Daily Spark Optimization

**Summary**
Optimized inefficient join, partitioning, and repeated subquery anti-patterns by pushing down filters, reordering joins, using CTEs, and pruning columns early.

**Files updated**
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filtering and column pruning reduce data shuffling and improve partition pruning.
- Factoring repeated subqueries into CTEs avoids recomputation and improves query planning.
- Optimized join order and hints ensure efficient join execution and resource usage.

> This PR was generated automatically. Please run tests and review carefully.
