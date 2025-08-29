## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, using CTEs for reuse, broadcasting small tables, and pruning columns before joins to reduce data movement and improve partition pruning.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filter application enables partition pruning and reduces scanned data
- Broadcasting small dimension tables minimizes shuffle in joins
- Column pruning and CTEs reduce redundant computation and memory usage

> This PR was generated automatically. Please run tests and review carefully.
