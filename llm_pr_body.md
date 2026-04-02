## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by applying partition pruning, predicate pushdown, column pruning, and CTEs for early filtering. This reduces data shuffling, avoids full table scans, and improves join efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Partition and predicate pushdown for large tables
- Column pruning and CTEs reduce data movement and computation
- Early filtering before joins improves join performance

> This PR was generated automatically. Please run tests and review carefully.
