## Daily Spark Optimization

**Summary**
Optimized anti-pattern SQL queries by pushing down filters, pruning columns, improving join order, and using CTEs for partition/pruning. This reduces data shuffling, enables partition pruning, and improves join efficiency.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and column pruning reduce I/O and memory usage
- Join order and filter pushdown minimize shuffle and intermediate data size
- CTEs make query plans more efficient and maintain identical output

> This PR was generated automatically. Please run tests and review carefully.
