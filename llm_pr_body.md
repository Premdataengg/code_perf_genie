## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL files by pushing down filters, pruning columns, materializing expensive CTEs, optimizing join order, and reducing window function passes. These changes improve partition pruning, reduce shuffles, and minimize memory and computation overhead.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`
- `src/queries/anti_patterns/10_anti_pattern_4_inefficient_aggregations.sql`
- `src/queries/anti_patterns/11_anti_pattern_5_memory_inefficient.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Pushed down filters and pruned columns early for partition pruning and reduced data movement.
- Materialized expensive aggregations to simulate caching and avoid recomputation.
- Optimized join order and used broadcast hints only for small tables.
- Combined/reduced window function passes and replaced subqueries with window functions where possible.

> This PR was generated automatically. Please run tests and review carefully.
