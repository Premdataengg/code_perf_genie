## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters into CTEs for partition pruning, reducing join sizes, and selecting only necessary columns. Broadcast hints are applied only to small tables.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Partition pruning and early filtering reduce I/O and shuffle.
- Join size reduction and column pruning improve memory and execution speed.

> This PR was generated automatically. Please run tests and review carefully.
