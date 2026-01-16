## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join order, predicate pushdown, column pruning, and reducing redundant window/aggregation computations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Explicit column selection and filter pushdown for partition pruning
- Join reordering and broadcast hints for efficient joins
- Reuse of expensive aggregations via CTEs to simulate caching
- Reduced redundant window functions and aggregations

> This PR was generated automatically. Please run tests and review carefully.
