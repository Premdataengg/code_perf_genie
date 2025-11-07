## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by improving partition pruning, join order, predicate pushdown, column pruning, and reducing repeated computation of expensive aggregations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Added early filtering and explicit column selection for partition pruning and column pruning.
- Reordered joins and added CTEs for predicate pushdown and efficient join strategy.
- Reduced repeated computation by referencing expensive CTEs only once, simulating caching.

> This PR was generated automatically. Please run tests and review carefully.
