## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters early for partition and predicate pruning, and by pruning columns before joins. Broadcast hints and join order are preserved for correctness.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Early filtering enables partition pruning and reduces data scanned
- Column pruning reduces shuffle and memory usage
- Predicate pushdown improves query performance by minimizing intermediate data

> This PR was generated automatically. Please run tests and review carefully.
