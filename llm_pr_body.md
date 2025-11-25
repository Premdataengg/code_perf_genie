## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by improving partition pruning, join order, predicate pushdown, column pruning, and simulating caching with CTEs.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Explicit column selection and filter pushdown reduce data scanned and improve partition pruning.
- Join order and broadcast hints now target only small tables, reducing shuffle and join cost.
- Expensive aggregations are computed once using CTEs, simulating caching and avoiding recomputation.

> This PR was generated automatically. Please run tests and review carefully.
