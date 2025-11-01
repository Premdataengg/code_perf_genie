## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by improving partition pruning, join order, predicate pushdown, window function efficiency, and simulated caching to reduce redundant computation.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Explicit column selection and partition pruning reduce I/O and improve scan efficiency.
- Join order and predicate pushdown minimize shuffle and data movement.
- Materializing expensive CTEs simulates caching, reducing recomputation.
- Window function and aggregation reductions lower computational overhead.

> This PR was generated automatically. Please run tests and review carefully.
