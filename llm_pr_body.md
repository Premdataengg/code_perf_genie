## Daily Spark Optimization

**Summary**
Optimized anti-pattern queries by pushing filters before joins and scans, pruning columns early, and using efficient window frames. This enables partition pruning, reduces data shuffling, and improves join performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Pushed filters into base queries for predicate and partition pruning
- Pruned columns before joins to reduce data transfer
- Used window frame for running_total for better performance
- Broadcasted small dimension tables for efficient joins

> This PR was generated automatically. Please run tests and review carefully.
