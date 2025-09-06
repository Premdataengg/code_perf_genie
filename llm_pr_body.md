## Daily Spark Optimization

**Summary**
Optimized inefficient join strategy by filtering large tables early, applying broadcast only to the small table, and pushing predicates before joins for partition pruning.

**Files updated**
- `src/queries/anti_patterns/08_anti_pattern_2_inefficient_joins.sql`


**Notes**
- Join order and predicate pushdown reduce data shuffled and scanned
- Broadcast join used only for small departments table for efficiency

> This PR was generated automatically. Please run tests and review carefully.
