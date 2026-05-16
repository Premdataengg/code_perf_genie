## Daily Spark Optimization

**Summary**
Optimized inefficient queries by pushing filters early for partition pruning, reordering joins, and pruning columns. Broadcast hints and predicate pushdown are applied for better join and scan performance.

**Files updated**
- `src/queries/anti_patterns/07_anti_pattern_1_no_partitioning.sql`
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Filters applied before joins for partition pruning
- Broadcast only small dimension tables
- Column pruning and early filtering reduce data shuffling and scanning

> This PR was generated automatically. Please run tests and review carefully.
