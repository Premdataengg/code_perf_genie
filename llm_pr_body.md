## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns, improving join strategies, simulating caching, and reducing redundant aggregations and subqueries.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter and column pruning reduces data volume and improves partition pruning.
- Join order and broadcast hints now follow best practices, reducing shuffle.
- Redundant window and aggregation computations are minimized via CTEs.
- Correlated subqueries replaced with joins for better memory efficiency.

> This PR was generated automatically. Please run tests and review carefully.
