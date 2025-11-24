## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, pruning columns, reordering joins, reducing redundant window functions, and simulating caching via CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and shuffle.
- Join order and predicate pushdown minimize data movement and improve join efficiency.
- Precomputing aggregations and using CTEs avoids repeated computation and speeds up queries.

> This PR was generated automatically. Please run tests and review carefully.
