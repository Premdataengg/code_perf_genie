## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing down filters, reducing repeated scans, and improving join order and broadcast usage. This enables partition pruning, minimizes data shuffling, and avoids unnecessary computation.

**Files updated**
- `src/queries/anti_patterns/06_performance_anti_pattern.sql`
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Pushes down predicates for partition pruning and early filtering
- Eliminates repeated subqueries and unnecessary CTEs
- Improves join order and broadcast hint placement for efficient joins

> This PR was generated automatically. Please run tests and review carefully.
