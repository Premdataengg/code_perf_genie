## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, pruning columns early, materializing expensive subqueries with CTEs, and adding broadcast hints where appropriate.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/09_anti_pattern_3_no_caching.sql`


**Notes**
- Materialized repeated subqueries and filtered datasets with CTEs for reuse and reduced recomputation
- Column pruning and filter pushdown reduce memory and shuffle, improving overall query performance

> This PR was generated automatically. Please run tests and review carefully.
