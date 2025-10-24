## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, pruning columns, improving join order, and reducing redundant computations and window functions. Caching is simulated via CTE reuse.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Partition pruning and column pruning reduce scanned data and shuffle.
- Join order and broadcast hints minimize data movement.
- Precomputing and reusing aggregates/window functions reduces computation.
- Simulated caching avoids recomputation of expensive CTEs.

> This PR was generated automatically. Please run tests and review carefully.
