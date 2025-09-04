## Daily Spark Optimization

**Summary**
Optimized inefficient join strategy by filtering large tables early, using CTEs for predicate pushdown, and applying broadcast only to the small table.

**Files updated**
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Early filtering reduces data shuffled in joins
- Broadcast join only for small departments table
- Column and partition pruning improves performance

> This PR was generated automatically. Please run tests and review carefully.
