## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join order, predicate pushdown, column pruning, and simulating caching of expensive computations.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Pushed down filters and selected only needed columns for better partition pruning and columnar efficiency.
- Reordered joins and pushed predicates to reduce data shuffling and improve join performance.
- Simulated caching of expensive aggregations to avoid recomputation and improve query speed.

> This PR was generated automatically. Please run tests and review carefully.
