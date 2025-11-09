## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join order, predicate pushdown, column pruning, and reducing repeated computations via CTE materialization.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Explicit column selection and window frame improves scan and window function efficiency
- Join reordering and predicate pushdown reduce data shuffle and improve join performance
- Materializing expensive CTEs simulates caching, reducing recomputation
- Precomputing window aggregations and pruning columns reduces multiple passes over data

> This PR was generated automatically. Please run tests and review carefully.
