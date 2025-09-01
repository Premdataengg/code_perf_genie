## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early for partition/pruning, using CTEs for predicate pushdown, and pruning columns. Broadcast hints and join order are preserved where appropriate.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`


**Notes**
- Early filtering enables partition pruning and reduces data scanned
- CTEs avoid redundant scans and improve query plan efficiency
- Column pruning reduces I/O and memory usage
- Maintains identical output and logic

> This PR was generated automatically. Please run tests and review carefully.
