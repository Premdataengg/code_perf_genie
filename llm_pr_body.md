## Daily Spark Optimization

**Summary**
Optimized inefficient anti-pattern queries by pushing filters early for partition pruning, column pruning, and predicate pushdown, and by restructuring joins for better performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Early filtering enables partition pruning and reduces data shuffled in joins
- Column pruning and CTEs avoid unnecessary data movement and computation
- Broadcast join hint is retained only for small tables

> This PR was generated automatically. Please run tests and review carefully.
