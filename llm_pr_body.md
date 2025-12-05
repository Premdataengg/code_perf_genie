## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, pruning columns, materializing reused CTEs, and reducing redundant window/aggregation computations for improved Spark SQL performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`


**Notes**
- Filter pushdown and column pruning reduce data scanned and shuffled.
- Materializing CTEs simulates caching to avoid recomputation.
- Precomputing and reusing aggregations/window functions minimizes redundant work.
- Join order and early filtering enable dynamic partition pruning and efficient execution.

> This PR was generated automatically. Please run tests and review carefully.
